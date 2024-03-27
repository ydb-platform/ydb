# View

A view is basically a query that is stored in a database which enables you to treat the results of the query as a table. The view itself contains no data. The results of a view are generated every time you select from that view. Any changes in the underlying tables are reflected immediately in the view.

Views are often used to:
- hide query complexity,
- limit access to underlaying data*,
- provide a backward compatible interface to emulate a table that used to exist, but whose schema has changed.

\* The scenario of creating a view to grant other users partial select privileges on a table that has sensitive data is not implemented yet. In {{ ydb-short-name }} view's stored query can only be executed on behalf of the user of the view. A user cannot select from a view that reads from a table that they don't have select privileges from. See `security_invoker` option description on the [CREATE VIEW](../../yql/reference/syntax/create_view.md) page for details.

## Creating a view

See [CREATE VIEW](../../yql/reference/syntax/create_view.md).

## Altering a view

See [ALTER VIEW](../../yql/reference/syntax/alter_view.md).

## Dropping a view

See [DROP VIEW](../../yql/reference/syntax/drop_view.md).

## View invalidation

If you drop a table that the view references, the view will become invalid. The queries through it will fail with an error of referencing a table that does not exist. The dependencies of views on the tables (and other views) are not tracked in any way at the moment. Select from a view is executed in the same manner as a select from a subquery would, without any prior checks of validity. You will know that the view's query became invalid only at the moment of its execution. This is going to change in the future releases. We are going to start tracking view's dependencies and the default behavior for dropping a table would be to forbid it if any view is referencing the table.

## Performance

Users might notice a little increase in the compilation time of the queries made using views compared to the compilation time of the same query, where views are substituted by their queries. It happens due to the fact that a statement reading from a view:
```sql
SELECT * FROM a_view;
```
is compiled similarly to a statement reading from a subquery:
```sql
SELECT * FROM (SELECT * FROM underlying_table);
```
but with an additional overhead of loading data from the schema object `a_view`.

Please note that if you execute the same query over and over again like:
```sql
-- execute multiple times
SELECT * FROM hot_view;
```
compilation results will be cached on the {{ ydb-short-name }} server and you will not notice any decrease of performance of queries using views.

Execution time of the resulting compiled code for queries using views should always be exactly the same as for the queries directly reading data from the underlying tables.

## View redefinition lag

### Query compilation cache
{{ ydb-short-name }} caches query compilation results on the server side for efficiency. For small queries like:
```sql
SELECT 1;
```
compilation can take up to a hundred times more CPU time than the execution. The cache entry is searched by the text of the query and some additional parameters such as a user SID.

The cache is updated by {{ ydb-short-name }} automatically to stay on track with the changes made to the objects that the query references. However, in the case of views the cache is not updated in the same transaction in which the object's definition has changed. It happens with a little delay.

### Problem statement

Imagine the following sitiuation:

Alice continiously executes the following query:
```sql
-- Alice's session
SELECT * FROM some_view_which_is_going_to_be_redefined;
```
while Bob redefines the view's query like this:
```sql
-- Bob's session
DROP VIEW some_view_which_is_going_to_be_redefined;
CREATE VIEW some_view_which_is_going_to_be_redefined ...;
```

The text of the Alice's query does not change, which means that the compilation will happen only once and the results are going to be taken from the cache since then. Bob changes the definition of the view and the cache entry for the Alice's query should theoretically be evicted from the cache in the same transaction, in which the view was redefined. However, this is not the case. The Alice's query will be recompiled with a little delay, which means that for a small period of time Alice's query will produce results inconsistent with the updated definition of the view. This is going to be fixed in the future releases.
