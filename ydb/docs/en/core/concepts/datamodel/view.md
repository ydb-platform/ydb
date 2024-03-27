# View

A view is a query which is treated as if it was a table storing the results of the query. The view itself contains no data. The content of a view is generated every time you select from that view. Any changes in the underlying tables are reflected immediately in the view.

Views are often used to:
- hide query complexity,
- limit access to underlying data*,
- provide a backward compatible interface to emulate a table that used to exist, but whose schema has changed.

\* The scenario of creating a view to grant other users partial select privileges on a table that has sensitive data is not implemented yet. In {{ ydb-short-name }} view's stored query can only be executed on behalf of the user of the view. A user cannot select from a view that reads from a table that they don't have privileges to select from. See `security_invoker` [option](../../yql/reference/syntax/create_view.md#security_invoker) description on the CREATE VIEW page for details.

## Creating a view

See [CREATE VIEW](../../yql/reference/syntax/create_view.md).

## Altering a view

See [ALTER VIEW](../../yql/reference/syntax/alter_view.md).

## Dropping a view

See [DROP VIEW](../../yql/reference/syntax/drop_view.md).

## View invalidation

If you drop a table that a view references, the view will become invalid. Queries through it will fail with an error, caused by referencing a table that does not exist. To make this view valid again, you need to provide (create or rename, for example) a selectable object (a table or a view) with the same name (and schema if it was specified in the view's query) that the deleted table had. The dependencies of views on tables (and other views) are not tracked in any way at the moment. Select from a view is executed in the same manner as a select from a subquery would: without any prior checks of validity. You would know that the view's query became invalid only at the moment of its execution. This is going to change in future releases. We are going to start tracking view's dependencies and the default behavior would be to forbid dropping a table if it has a view referencing it.

## Query execution context

See [notes](../../yql/reference/syntax/create_view.md#notes) on the CREATE VIEW page. Search by the word `context`.

## Performance

Queries are executed in 2 steps:
1. compilation,
2. execution of the compiled code.

The resultant compiled code contains no evidence that the query was made using views, because all the references to views should have been replaced during compilation by the queries that they represent. In practice, it means that there must be no difference in the execution time of the compiled code (step №2) for queries made using views vs. queries directly reading from the underlying tables.

However, users might notice a little increase in the compilation time of the queries made using views compared to the compilation time of the same queries written directly. It happens due to the fact that a statement reading from a view:
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
compilation results will be cached on the {{ ydb-short-name }} server and you will not notice any difference in performance of queries using views and direct queries.

## View redefinition lag

### Query compilation cache
{{ ydb-short-name }} caches query compilation results on the server side for efficiency. For small queries like:
```sql
SELECT 1;
```
compilation can take up to a hundred times more CPU time than the execution. The cache entry is searched by the text of the query and some additional information such as a user SID.

The cache is updated by {{ ydb-short-name }} automatically to stay on track with the changes made to the objects that the query references. However, in the case of views the cache is not updated in the same transaction in which the object's definition changes. It happens with a little delay.

### Problem statement

Imagine the following situation.

Alice continuously executes the following query:
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

The text of the Alice's query does not change, which means that the compilation will happen only once and the results are going to be taken from the cache since then. Bob changes the definition of the view and the cache entry for the Alice's query should theoretically be evicted from the cache in the same transaction, in which the view was redefined. However, this is not the case. The Alice's query will be recompiled with a little delay, which means that for a small period of time Alice's query will produce results inconsistent with the updated definition of the view. This is going to be fixed in future releases.
