#include "postgres.h"

#include "access/htup_details.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "common/relpath.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/primnodes.h"
#include "storage/bufmgr.h"
#include "storage/itemid.h"
#include "storage/itemptr.h"
#include "utils/builtins.h"
#include "utils/elog.h"

PG_MODULE_MAGIC;

#define RESULT_ARG_NUM 2

Datum get_bloat(PG_FUNCTION_ARGS);

Datum get_bloat(PG_FUNCTION_ARGS) {
    char *relschema = text_to_cstring(PG_GETARG_TEXT_P(0));
    char *relname = text_to_cstring(PG_GETARG_TEXT_P(1));
    RangeVar *r_var;
    Relation rel;
    uint64 table_size;
    int64 num_dead_tuples;
    BlockNumber blkno, num_blocks;
    Buffer buffer, buffer2;
    Page page, page2;
    ItemId lp;
    Datum result;
    Datum values[RESULT_ARG_NUM];
    bool nulls[RESULT_ARG_NUM] = {false};
    TupleDesc tupdesc;
    HeapTuple tuple;
    HeapTupleHeader tuphdr, tuphdr2;
    TransactionId curr_xmax = InvalidTransactionId;

    /* superuser??? */
    if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use bloat function")));

    r_var = makeRangeVar(relschema, relname, -1);
    rel = table_openrv(r_var, AccessShareLock);
    if(!rel)
        elog(ERROR, "Could not open table(%s). Accessing cross-database tables is not allowed", quote_qualified_identifier(relschema, relname));

    table_size = table_block_relation_size(rel, MAIN_FORKNUM);
    if (!table_size) { /* No need to go through empty table */
        table_close(rel, AccessShareLock);
        elog(ERROR, "Empty table: %s.%s", relschema, relname);
    }
    
    num_blocks = (table_size / BLCKSZ);
    num_dead_tuples = 0;

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

    tupdesc = BlessTupleDesc(tupdesc);

    for (blkno = 0; blkno < num_blocks; blkno++)
    {
        OffsetNumber max_off, curr_off;
        ItemPointerData item;
        buffer = ReadBuffer(rel, blkno);

        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buffer);

        max_off = PageGetMaxOffsetNumber(page);
        if (!max_off){
            UnlockReleaseBuffer(buffer);
            elog(DEBUG1, "Page=%u may be empty", blkno);
            continue;
        }

        /* Line pointer array starts at 1 */
        curr_off = FirstOffsetNumber;

        for (; curr_off <= max_off; curr_off = OffsetNumberNext(curr_off))
        {
            lp = PageGetItemId(page, curr_off);
            if(!ItemIdIsValid(lp)) {
                elog(DEBUG1, "Tuple item=%u in page=%u could not be read", curr_off, blkno);
                continue;
            }

            if(ItemIdIsDead(lp)) {
                num_dead_tuples++;
                continue;
            }

            tuphdr = (HeapTupleHeader)PageGetItem(page, lp);

            curr_xmax = HeapTupleHeaderGetUpdateXid(tuphdr); /* Get the ID of the updating transaction */
            ItemPointerSet(&item, blkno, curr_off);  /* Make self pointer */

            /* After an update, the old tuple points to the new tuple. This check ensures we are dealing with tuple updates */
            if (!ItemPointerEquals(&tuphdr->t_ctid, &item)) {
                ItemId lp2;

                /* If the new tuple is within the same block, we don't need to read a new page from disk/in-memory bufferpool */
                if (ItemPointerGetBlockNumber(&tuphdr->t_ctid) == blkno && ItemPointerGetOffsetNumber(&tuphdr->t_ctid) <= max_off)
                {
                    lp2 = PageGetItemId(page, ItemPointerGetOffsetNumber(&tuphdr->t_ctid));
                    if(!ItemIdIsValid(lp2)) {
                        elog(DEBUG1, "Tuple item=%u in page=%u could not be read", curr_off, blkno);
                        continue;
                    }

                    tuphdr2 = (HeapTupleHeader)PageGetItem(page, lp2);

                    if (TransactionIdIsValid(curr_xmax) && TransactionIdEquals(HeapTupleHeaderGetXmin(tuphdr2), curr_xmax))
                    {
                        num_dead_tuples++;
                    }
                } else {
                    /* New tuple is in another page. We have to read the page in if it wasn't in the bufferpool already 
                     * We have to get the new page and get a shared lock to read it's contents
                    */

                    if(ItemPointerGetBlockNumber(&tuphdr->t_ctid) > num_blocks) /* Ensure the don't read past the size of the table */
                        continue;

                    buffer2 = ReadBuffer(rel, ItemPointerGetBlockNumber(&tuphdr->t_ctid));

                    LockBuffer(buffer2, BUFFER_LOCK_SHARE);
                    page2 = BufferGetPage(buffer2);

                    lp2 = PageGetItemId(page2, ItemPointerGetOffsetNumber(&tuphdr->t_ctid));
                    if(!ItemIdIsValid(lp2)) {
                        UnlockReleaseBuffer(buffer2);
                        elog(DEBUG1, "Tuple item=%u in page=%u could not be read", curr_off, blkno);
                        continue;
                    }

                    tuphdr2 = (HeapTupleHeader)PageGetItem(page2, lp2);

                    if (TransactionIdIsValid(curr_xmax) && TransactionIdEquals(HeapTupleHeaderGetXmin(tuphdr2), curr_xmax))
                    {
                        if (HeapTupleHeaderXminCommitted(tuphdr2))
                        {
                            /* Txn still in progress */
                        }

                        num_dead_tuples++;
                    }

                    UnlockReleaseBuffer(buffer2);
                    
                }
                
            } else if(ItemPointerEquals(&tuphdr->t_ctid, &item) && TransactionIdIsValid(curr_xmax)) { /* Deleted tuples would have their tid pointing to self and xmax set */
                num_dead_tuples++;
            }
        }
        UnlockReleaseBuffer(buffer);
    }

    values[0] = CStringGetTextDatum(RelationGetRelationName(rel));
    values[1] = Int64GetDatum(num_dead_tuples);

    tuple = heap_form_tuple(tupdesc, values, nulls);
    result = HeapTupleGetDatum(tuple);

    table_close(rel, AccessShareLock);

    PG_RETURN_DATUM(result);
    
}

PG_FUNCTION_INFO_V1(get_bloat);
