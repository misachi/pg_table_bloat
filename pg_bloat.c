#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/itup.h"
#include "access/nbtree.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "catalog/index.h"
#include "common/relpath.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "storage/bufmgr.h"
#include "storage/itemid.h"
#include "storage/itemptr.h"
#include "utils/builtins.h"
#include "utils/elog.h"

PG_MODULE_MAGIC;

#define RESULT_ARG_NUM 4
#define MAX_DEAD_ITEM_ARRAY_SIZE 1024

typedef struct IndexScanResult
{
    Relation *rels;
    ListCell *last_scan_rel;
    int64 ndead;
    int item_off;
    ItemPointer dead_items;
} IndexScanResult;

static bool reap_tid(ItemPointer itemptr, IndexScanResult *scan);
static int cmp_itemptr(const void *left, const void *right);
void scan_index(IndexScanResult *scan, List *rel_list);
void mark_deleted_index_tuples(IndexScanResult *scan, List *rel_list, BlockNumber blkno, OffsetNumber *off);
Datum get_bloat(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(get_bloat);
Datum get_bloat(PG_FUNCTION_ARGS)
{
    char *relschema = text_to_cstring(PG_GETARG_TEXT_P(0));
    char *relname = text_to_cstring(PG_GETARG_TEXT_P(1));
    RangeVar *r_var;
    Relation rel;
    uint64 table_size;
    int64 num_dead_tuples, dead_tuple_size;
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
    IndexScanResult scan;
    List *indexTableList;

    /* superuser??? */
    if (!superuser())
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("must be superuser to use bloat function")));

    r_var = makeRangeVar(relschema, relname, -1);
    rel = table_openrv(r_var, AccessShareLock);
    if (!rel)
        elog(ERROR, "Could not open table(%s). Accessing cross-database tables is not allowed", quote_qualified_identifier(relschema, relname));

    table_size = table_block_relation_size(rel, MAIN_FORKNUM);
    if (!table_size)
    { /* No need to go through empty table */
        table_close(rel, AccessShareLock);
        elog(ERROR, "Empty table: %s.%s", relschema, relname);
    }

    num_blocks = (table_size / BLCKSZ);
    dead_tuple_size = num_dead_tuples = 0;

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    tupdesc = BlessTupleDesc(tupdesc);

    indexTableList = RelationGetIndexList(rel);
    scan.ndead = 0;
    scan.item_off = 0;
    scan.dead_items = (ItemPointer)palloc(MAX_DEAD_ITEM_ARRAY_SIZE * sizeof(ItemPointerData));

    for (blkno = 0; blkno < num_blocks; blkno++)
    {
        OffsetNumber max_off, curr_off;
        ItemPointerData item;
        buffer = ReadBuffer(rel, blkno);

        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buffer);

        max_off = PageGetMaxOffsetNumber(page);
        if (!max_off)
        {
            UnlockReleaseBuffer(buffer);
            elog(DEBUG1, "Page=%u may be empty", blkno);
            continue;
        }

        /* Line pointer array starts at 1 */
        curr_off = FirstOffsetNumber;

        for (; curr_off <= max_off; curr_off = OffsetNumberNext(curr_off))
        {
            lp = PageGetItemId(page, curr_off);
            if (!ItemIdIsValid(lp))
            {
                elog(DEBUG1, "Tuple item=%u in page=%u could not be read", curr_off, blkno);
                continue;
            }

            if (ItemIdIsDead(lp))
            {
                dead_tuple_size += ItemIdGetLength(lp);
                num_dead_tuples++;

                mark_deleted_index_tuples(&scan, indexTableList, blkno, &curr_off);
                continue;
            }

            tuphdr = (HeapTupleHeader)PageGetItem(page, lp);

            curr_xmax = HeapTupleHeaderGetRawXmax(tuphdr); /* Get the ID of the updating transaction */
            ItemPointerSet(&item, blkno, curr_off);        /* Make self pointer */

            /* After an update, the old tuple points to the new tuple. This check ensures we are dealing with tuple updates */
            if (!ItemPointerEquals(&tuphdr->t_ctid, &item))
            {
                ItemId lp2;

                /* If the new tuple is within the same block, we don't need to read a new page from disk/in-memory bufferpool */
                if (ItemPointerGetBlockNumber(&tuphdr->t_ctid) == blkno && ItemPointerGetOffsetNumber(&tuphdr->t_ctid) <= max_off)
                {
                    lp2 = PageGetItemId(page, ItemPointerGetOffsetNumber(&tuphdr->t_ctid));
                    if (!ItemIdIsValid(lp2))
                    {
                        elog(DEBUG1, "Tuple item=%u in page=%u could not be read", curr_off, blkno);
                        continue;
                    }

                    tuphdr2 = (HeapTupleHeader)PageGetItem(page, lp2);

                    if (TransactionIdIsValid(curr_xmax) && TransactionIdEquals(HeapTupleHeaderGetXmin(tuphdr2), curr_xmax))
                    {
                        dead_tuple_size += ItemIdGetLength(lp);
                        num_dead_tuples++;
                        mark_deleted_index_tuples(&scan, indexTableList, blkno, &curr_off);
                    }
                }
                else
                {
                    /* New tuple is in another page. We have to read the page in if it wasn't in the bufferpool already
                     * We have to get the new page and get a shared lock to read it's contents
                     */

                    if (ItemPointerGetBlockNumber(&tuphdr->t_ctid) > num_blocks) /* Ensure the don't read past the size of the table */
                        continue;

                    buffer2 = ReadBuffer(rel, ItemPointerGetBlockNumber(&tuphdr->t_ctid));

                    LockBuffer(buffer2, BUFFER_LOCK_SHARE);
                    page2 = BufferGetPage(buffer2);

                    lp2 = PageGetItemId(page2, ItemPointerGetOffsetNumber(&tuphdr->t_ctid));
                    if (!ItemIdIsValid(lp2))
                    {
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
                        dead_tuple_size += ItemIdGetLength(lp);
                        num_dead_tuples++;
                        mark_deleted_index_tuples(&scan, indexTableList, blkno, &curr_off);
                    }

                    UnlockReleaseBuffer(buffer2);
                }
            }
            else if (ItemPointerEquals(&tuphdr->t_ctid, &item) && TransactionIdIsValid(curr_xmax))
            { /* Deleted tuples would have their tid pointing to self and xmax set */
                dead_tuple_size += ItemIdGetLength(lp);
                num_dead_tuples++;
                mark_deleted_index_tuples(&scan, indexTableList, blkno, &curr_off);
            }
        }
        UnlockReleaseBuffer(buffer);
    }

    if (num_dead_tuples > 0)
        scan_index(&scan, indexTableList);

    values[0] = CStringGetTextDatum(RelationGetRelationName(rel));
    values[1] = Int64GetDatum(num_dead_tuples);
    values[2] = Int64GetDatum(dead_tuple_size);
    values[3] = Int64GetDatum(scan.ndead);

    tuple = heap_form_tuple(tupdesc, values, nulls);
    result = HeapTupleGetDatum(tuple);

    list_free(indexTableList);
    pfree(scan.dead_items);
    table_close(rel, AccessShareLock);

    PG_RETURN_DATUM(result);
}

/*
 * Compare item pointers
 * copied from commands/vacuum.c: vac_cmp_itemptr
 */
static int
cmp_itemptr(const void *left, const void *right)
{
    BlockNumber lblk,
        rblk;
    OffsetNumber loff,
        roff;

    lblk = ItemPointerGetBlockNumber((ItemPointer)left);
    rblk = ItemPointerGetBlockNumber((ItemPointer)right);

    if (lblk < rblk)
        return -1;
    if (lblk > rblk)
        return 1;

    loff = ItemPointerGetOffsetNumber((ItemPointer)left);
    roff = ItemPointerGetOffsetNumber((ItemPointer)right);

    if (loff < roff)
        return -1;
    if (loff > roff)
        return 1;

    return 0;
}

/*
 * copied from commands/vacuum.c: vac_tid_reaped
 */
static bool reap_tid(ItemPointer itemptr, IndexScanResult *scan)
{
    int64 litem, ritem, item;
    ItemPointer res;

    litem = itemptr_encode(&scan->dead_items[0]);
    ritem = itemptr_encode(&scan->dead_items[scan->item_off - 1]);
    item = itemptr_encode(itemptr);

    if (item < litem || item > ritem)
        return false;

    res = (ItemPointer)bsearch(itemptr,
                               scan->dead_items,
                               scan->item_off,
                               sizeof(ItemPointerData),
                               cmp_itemptr);

    return (res != NULL);
}

void scan_index(IndexScanResult *scan, List *rel_list)
{
    ListCell *lcell = scan->last_scan_rel;
    foreach (lcell, rel_list)
    {
        Oid index_oid = lfirst_oid(lcell);
        Relation rel = index_open(index_oid, AccessShareLock);
        Page page;
        BTPageOpaque opaque;

        if (rel->rd_index->indisready)
        {
            uint64 table_size;
            BlockNumber blkno, num_blocks;
            table_size = table_block_relation_size(rel, MAIN_FORKNUM);
            if (!table_size)
            {
                index_close(rel, AccessShareLock);
                continue;
            }

            num_blocks = (table_size / BLCKSZ);

            for (blkno = 0; blkno < num_blocks; blkno++)
            {
                Buffer buf;
                OffsetNumber max_off, curr_off;
                buf = ReadBuffer(rel, blkno);

                /* Only doing a scan; no need for a cleanup lock */
                _bt_lockbuf(rel, buf, BT_READ);
                page = BufferGetPage(buf);
                opaque = BTPageGetOpaque(page);

                max_off = PageGetMaxOffsetNumber(page);
                if (!max_off)
                {
                    _bt_relbuf(rel, buf);
                    elog(DEBUG1, "Index Page=%u may be empty", blkno);
                    continue;
                }

                /* The first key is Hikey, we start at second key */
                curr_off = P_FIRSTDATAKEY(opaque);
                for (; curr_off <= max_off; curr_off = OffsetNumberNext(curr_off))
                {
                    IndexTuple itup = (IndexTuple)PageGetItem(page, PageGetItemId(page, curr_off));
                    if (reap_tid(&itup->t_tid, scan))
                        scan->ndead++;
                }

                _bt_relbuf(rel, buf);
            }
        }
        index_close(rel, AccessShareLock);
    }
    scan->last_scan_rel = lcell;
    memset(scan->dead_items, 0, MAX_DEAD_ITEM_ARRAY_SIZE * sizeof(ItemPointerData));
    scan->item_off = 0;
}

void mark_deleted_index_tuples(IndexScanResult *scan, List *rel_list, BlockNumber blkno, OffsetNumber *off)
{
    if (scan->item_off < MAX_DEAD_ITEM_ARRAY_SIZE)
    {
        ItemPointerSet(&scan->dead_items[scan->item_off], blkno, *off);
        scan->item_off++;
    }
    else
    {
        scan_index(scan, rel_list);
        (*off)--;
    }
}
