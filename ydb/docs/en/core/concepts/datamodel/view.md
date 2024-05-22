# View

A view logically represents a table formed by a given query. The view itself contains no data. The content of a view is generated every time you `SELECT` from it. Thus, any changes in the underlying tables are reflected immediately in the view.

Views are often used to:

- hide query complexity
- limit access to underlying data
- provide a backward-compatible interface to emulate a table that used to exist but whose schema has changed

{% note warning %}

The scenario of creating a view to grant other users partial `SELECT` privileges on a table that has sensitive data has not been implemented yet. In {{ ydb-short-name }}, the view's stored query can only be executed on behalf of the user querying the view. A user cannot access data from a view that reads from a table that they don't have privileges to `SELECT` from. See the `security_invoker` [option](../../yql/reference/syntax/create-view.md#security_invoker) description on the `CREATE VIEW` page for details.

{% endnote %}

## View invalidation

If you drop a table that a view references, the view will become invalid. Queries against it will fail with an error caused by referencing a table that does not exist. To make the view valid again, you must provide a queryable entity with the same name (by creating or renaming a table or another view). It needs to have a schema compatible with the deleted one. The dependencies of views on tables and other views are not tracked. A `SELECT` from a view is executed like a `SELECT` from a subquery would, without any prior checks of validity. You would know that the view's query became invalid only at the moment of its execution. This approach will change in future releases: {{ ydb-short-name }} will start tracking the view's dependencies, and the default behavior would be to forbid dropping a table if there's a view referencing it.

## Performance

Queries are executed in two steps:
1. compilation
2. execution of the compiled code

The resulting compiled code contains no evidence that the query was made using views because all the references to views should have been replaced during compilation by the queries that they represent. In practice, there must be no difference in the execution time of the compiled code (step 2) for queries made using views versus queries directly reading from the underlying tables.

However, users might notice a little increase in the compilation time of the queries made using views compared to the compilation time of the same queries written directly. It happens because a statement reading from a view:
```sql
SELECT * FROM a_view;
```
is compiled similarly to a statement reading from a subquery:
```sql
SELECT * FROM (SELECT * FROM underlying_table);
```
but with an additional overhead of loading data from the schema object `a_view`.

Please note that if you execute the same query over and over again, like:
```sql
-- execute multiple times
SELECT * FROM hot_view;
```
compilation results will be cached on the {{ ydb-short-name }} server side, and you will not notice any difference in the performance of queries using views and direct queries.

## View redefinition lag

{% note warning %}

Execution plans of queries containing views are currently cached. It might lead to the usage of an old query plan for a short while after a given view has been redefined. This is going to be fixed in future releases. See below for a more detailed explanation.

{% endnote %}

### Query compilation cache

{{ ydb-short-name }} caches query compilation results on the server side for efficiency. For small queries like `SELECT 1;` compilation can take significantly more time than the execution.

The cache entry is searched by the text of the query and some additional information, such as a user SID.

The cache is automatically updated by {{ ydb-short-name }} to stay on track with the changes made to the objects the query references. However, in the case of views, the cache is not updated in the same transaction in which the object's definition changes. It happens with a little delay.

### Example of the problem

Let's consider the following situation. Alice repeatedly executes the following query:
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

The text of Alice's query does not change, which means that the compilation will happen only once, and the results are going to be taken from the cache since then. Bob changes the definition of the view, and the cache entry for Alice's query should theoretically be evicted from the cache in the same transaction in which the view was redefined. However, this is not the case. Alice's query will be recompiled with a little delay, which means that for a short period of time, Alice's query will produce results that are inconsistent with the updated definition of the view. This is going to be fixed in future releases.

## See also

* [CREATE VIEW](../../yql/reference/syntax/create-view.md)
* [ALTER VIEW](../../yql/reference/syntax/alter-view.md)
* [DROP VIEW](../../yql/reference/syntax/drop-view.md)