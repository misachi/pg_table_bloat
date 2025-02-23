# pg_table_bloat

A simple extension to get an estimate of table bloat -- number of tuples that updated/deleted but still occupying space and would be removed by vacuum process

To install
```
create extension pg_table_bloat;
```

Example usage
```
postgres=# create table foo(id int, descr text);  -- New table(or use an existing one)
CREATE TABLE
postgres=# insert into foo values(1, 'Hello'); -- New row
INSERT 0 1
postgres=# insert into foo values(2, 'World');
INSERT 0 1
postgres=# select * from foo;
 id | descr
----+-------
  1 | Hello
  2 | World
(2 rows)

postgres=# update foo set descr='Hello World' where id=1;  -- Update an existing record
UPDATE 1
postgres=# select * from get_bloat('public', 'foo');  -- pg_table_bloat shows a potentially dead tuple as 1
 relname | num_dead_tuples
---------+-----------------
 foo     |               1
(1 row)

postgres=#
postgres=# delete from foo where id=2;  -- deleting a record leaves a dead tuple around
DELETE 1

postgres=# select ctid, id, descr from foo; -- The ctid column shows we now have one record with one block 0(first block) and 3rd item(item 1 is the old record referencing this and item 2 has been deleted)
 ctid  | id |    descr
-------+----+-------------
 (0,3) |  1 | Hello World
(1 row)

postgres=# select * from get_bloat('public', 'foo'); -- pg_table_bloat shows potentially dead tuples as 2
 relname | num_dead_tuples
---------+-----------------
 foo     |               2
(1 row)

postgres=# vacuum foo; -- Cleanup table and remove all dead tuples, freeing up space
VACUUM
postgres=# select * from get_bloat('public', 'foo'); -- Now bloat is removed after vacuum
 relname | num_dead_tuples
---------+-----------------
 foo     |               0
(1 row)

```
