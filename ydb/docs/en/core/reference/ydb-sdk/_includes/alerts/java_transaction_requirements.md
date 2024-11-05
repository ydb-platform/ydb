{% note info %}

Transaction requirements:

* It should be an active transaction (that has an id) from one of {{ ydb-short-name }} services. I.e., [Table](https://github.com/ydb-platform/ydb-java-sdk/blob/master/table/src/main/java/tech/ydb/table/transaction/TableTransaction.java) or [Query](https://github.com/ydb-platform/ydb-java-sdk/blob/master/query/src/main/java/tech/ydb/query/QueryTransaction.java).
* Only the `SERIALIZABLE_RW` transaction isolation level is supported in the Topic Service.

{% endnote %}