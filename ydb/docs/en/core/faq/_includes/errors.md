# Errors

#### What do I do if I frequently get the "Transaction locks invalidated" error? {#locks-invalidated}

Typically, if you get this error, repeat a transaction, as {{ ydb-short-name }} uses optimistic locking. If this error occurs frequently, this is the result of a transaction reading a large number of rows or of many transactions competing for the same "hot" rows. It makes sense to view the queries running in the transaction and check if they're reading unnecessary rows.

#### What causes the "Exceeded maximum allowed number of active transactions" error? {#exceed-number-transactions}

The logic on the client side should try to keep transactions as short as possible.

No more than 10 active transactions are allowed per session. When starting a transaction, use either the commit flag for autocommit or an explicit commit/rollback.

