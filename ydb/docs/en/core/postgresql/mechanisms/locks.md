# Locks

Locks are used to coordinate concurrent access of multiple processes to the same resources. A resource can be an object that the DBMS works with:

1. Primary key lists, conditions, or the entire datashard (if there are too many locks)
2. An in-memory structure, such as a hash table, buffer, etc. (identified by a pre-assigned number)
3. Abstract resources that have no physical meaning (identified simply by a unique number)

For a process to access a resource, it must acquire the lock associated with that resource. A lock is a piece of shared memory that stores information about whether the lock is free or acquired. The lock itself is also a resource that can be accessed concurrently. Special synchronization primitives are used to access locks: semaphores or mutexes. Their purpose is to ensure that the code accessing the shared resource is executed in only one process at a time. If the resource is busy and the lock cannot be acquired, the process will terminate with an error.

Locks can be classified by their implementation principle:

- **Pessimistic locking**: before data modification, a lock is placed on all potentially affected rows. This prevents other sessions from modifying them until the current operation completes. After modification, data writing is guaranteed to be consistent
- **Optimistic locking** does not restrict data access during operation but uses a special attribute (for example: `VERSION`) to control changes. The attribute is a field in the row metadata that is invisible to users; it belongs to the implementation details of the locking mechanism. Before committing changes, the set attribute is checked. If it has not changed, the changes are committed (`COMMIT`); otherwise, the transaction is rolled back (`ROLLBACK`)

In {{ ydb-short-name }} compatibility with PostgreSQL, optimistic locking is used – this means that transactions check lock conditions at the end of their execution. If a lock was violated during the transaction, such a transaction will terminate with an error:


```text
Error: Transaction locks invalidated. Table: <table name>, code: 2001
```


Transactions that execute SQL read and write statements may terminate with an error. Transactions that execute only SQL read or write statements complete successfully. Let's give an example of a transaction that will terminate with an error if changes are made to the table data by a concurrent transaction:


```sql
BEGIN;
SELECT * FROM people;
-- If an INSERT is executed here in another transaction, this transaction will fail.
UPDATE people SET age = 27
WHERE name = 'JOHN';
COMMIT;
```


As a result, the transaction will terminate with error `Error: Transaction locks invalidated` and be rolled back (`ROLLBACK`). In case of error `Error: Transaction locks invalidated`, you can try to execute the transaction again.
