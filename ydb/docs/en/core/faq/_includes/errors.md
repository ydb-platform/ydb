# Errors

## Possible causes for "Status: OVERLOADED Error: Pending previous query completion" in the C++ SDK

Q: When running two queries, I try to get a response from the future method of the second one. It returns: `Status: OVERLOADED Why: <main>: Error: Pending previous query completion`.

A: Sessions in the SDK are single-threaded. To run multiple queries at once, you need to create multiple sessions.

## What do I do if I frequently get the "Transaction locks invalidated" error? {#locks-invalidated}

Typically, if you get this error, repeat a transaction, as {{ ydb-short-name }} uses optimistic locking. If this error occurs frequently, this is the result of a transaction reading a large number of rows or of many transactions competing for the same "hot" rows. It makes sense to view the queries running in the transaction and check if they're reading unnecessary rows.

## What causes the "Exceeded maximum allowed number of active transactions" error? {#exceed-number-transactions}

The logic on the client side should try to keep transactions as short as possible.

No more than 10 active transactions are allowed per session. When starting a transaction, use either the commit flag for autocommit or an explicit commit/rollback.

## What do I do if I get the Datashard: Reply size limit exceeded error in response to a query? {#reply-size-exceeded}

This error means that, as a query was running, one of the participating data shards attempted to return over 50 MB of data, which exceeds the allowed limit.

Recommendations:

* A general recommendation is to reduce the amount of data processed in a transaction.
* If a query involves a `Join`, it's a good idea to make sure that the method used is [Index lookup Join](../yql.md#index-lookup-join).
* If a simple selection is performed, make sure that it is done by keys, or add `LIMIT` in the query.

## What do I do is I get the "Datashard program size limit exceeded" in response to a query? {#program-size-exceeded}

This error means that the size of a program (including parameter values) exceeded the 50-MB limit for one of the data shards. In most cases, this indicates an attempt to write over 50 MB of data to database tables in a single transaction. All modifying operations in a transaction such as `UPSERT`, `REPLACE`, `INSERT`, or `UPDATE` count as records.

You need to reduce the total size of records in one transaction. Normally, we don't recommend combining queries that logically don't require transactionality in a single transaction. When adding/updating data in batches, we recommend reducing the size of one batch to values not exceeding a few megabytes.

