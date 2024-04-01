{% note info %}

Transaction requirements:

It should be an active transaction (that has id) from one of YDB services. I.e., [Table](https://github.com/ydb-platform/ydb-java-sdk/blob/master/table/src/main/java/tech/ydb/table/transaction/TableTransaction.java) or [Query](https://github.com/ydb-platform/ydb-java-sdk/blob/master/query/src/main/java/tech/ydb/query/QueryTransaction.java).

Only `SERIALIZABLE_RW` transaction isolation level is supported in Topic Service.

{% endnote %}