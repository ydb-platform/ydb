## Managing transactions {#tcl}

Transactions are managed through [TCL](../../../../concepts/transactions.md) `Begin` and `Commit` calls.

In most cases, instead of explicitly using `Begin` and `Commit` calls, it's better to use transaction control parameters in execute calls. This allows to avoid additional requests to {{ ydb-short-name }} server and thus run queries more efficiently.

