{% note info %}

Unlike PostgreSQL, YDB uses optimistic locking. This means that transactions check the conditions for the necessary locks at the end of their operation, not at the beginning. If the lock has been violated during the transaction's execution, such a transaction will end with a `Transaction locks invalidated` error. In this case, you can try to execute a similar transaction again.

{% endnote %}