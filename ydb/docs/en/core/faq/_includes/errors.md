# Errors

#### What should I do if I frequently get the "Transaction locks invalidated" error? {#locks-invalidated}

Typically, if you get this error, repeat a transaction, as {{ ydb-short-name }} uses optimistic locking. If this error occurs frequently, it means that a large number of rows are being read in a transaction or that many transactions are competing for the same "hot" rows. It makes sense to view the queries running in the transaction and check if they're reading unnecessary rows.

#### Why does the error "Exceeded maximum allowed number of active transactions" occur? {#exceed-number-transactions}

The logic on the client side should try to keep transactions as short as possible.

No more than 10 active transactions are allowed per session. When starting a transaction, use either the commit flag for autocommit or an explicit commit/rollback.

