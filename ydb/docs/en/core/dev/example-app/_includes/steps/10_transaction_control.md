## Managing transactions {#tcl}

Transactions are managed through [TCL](../../../../concepts/transactions.md) Begin and Commit calls.

In most cases, instead of explicitly using Begin and Commit calls, it's better to use transaction control parameters in execute calls. This helps you avoid unnecessary requests to {{ ydb-short-name }} and run your queries more efficiently.

