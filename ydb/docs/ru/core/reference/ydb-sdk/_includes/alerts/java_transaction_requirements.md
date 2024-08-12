{% note info %}

Требования к транзакции:

* Это должна быть активная (имеющая идентификатор) транзакция в одном из сервисов YDB. Например, [Table](https://github.com/ydb-platform/ydb-java-sdk/blob/master/table/src/main/java/tech/ydb/table/transaction/TableTransaction.java) или [Query](https://github.com/ydb-platform/ydb-java-sdk/blob/master/query/src/main/java/tech/ydb/query/QueryTransaction.java).

* В Topic Service поддерживается только уровень изоляции транзакций `SERIALIZABLE_RW`.

{% endnote %}