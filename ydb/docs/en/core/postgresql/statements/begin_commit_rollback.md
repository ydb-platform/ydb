# BEGIN, COMMIT, ROLLBACK (working with Transactions)

{% include [alert_preview](../_includes/alert_preview.md) %}

**Transactions** are a method of grouping one or more database operation into a single unit of work. A transaction may consist of one or several SQL statements and is used to ensure the consistency of data. A transaction guarantees that either all or none of the included SQL statements will be executed. Transactions are managed by the commands `BEGIN`, `COMMIT`, `ROLLBACK`.

Transactions are executed within sessions. A **session** is a single connection to the database, which begins when a client connects to the database and ends upon disconnection. A transaction starts with the `BEGIN` command and ends with the `COMMIT` command (for successful completion) or `ROLLBACK` (to revert). It is not obligatory to specify `BEGIN`, `COMMIT`, and `ROLLBACK` explicitly, they are implied if not specified. If a session is unexpectedly interrupted, then all uncommitted transaction that were initiated in the current session are automatically rolled back.

Let's review each of the commands:

* `BEGIN` initiates a new transaction. After this command is executed, all subsequent database operations are performed within the context of this transaction.
* `COMMIT` completes the current transaction by applying all of its operations. If all operations within the transaction are successful, the results of these operations are made permanent. The changes become visible to subsequent transactions.
* `ROLLBACK` reverts the current transaction, canceling all of its operations, if errors occurred during the transaction's execution or if the transaction is being aborted by the application based on its internal logic. When `ROLLBACK` is called, only the changes made within the current transaction are canceled. Changes made by other transactions (even if they were initiated and completed during the execution of the current transaction) remain unaffected. If an error occurs during the execution of a transaction, further operations within that transaction become impossible – a `ROLLBACK` must be done since performing a `COMMIT` would return an error. If a session is disconnected during an active transaction – a `ROLLBACK` will automatically be executed. For more detailed information about concurrency control (MVCC), refer to [this article](../../concepts/mvcc.md).


Suppose you need to make changes to different rows in a table for different columns so that the transaction is combined into a single unit of work and has the guarantees of [ACID](https://en.wikipedia.org/wiki/ACID). Such a record may look like this:

{% include [example](../_includes/transactions/example.md) %}

{% include [alert_locks](../_includes/alert_locks.md) %}