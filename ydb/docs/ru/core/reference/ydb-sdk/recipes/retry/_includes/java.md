В {{ ydb-short-name }} Java SDK механизм повторных запросов реализован в виде класс хелпера `com.yandex.ydb.table.SessionRetryContext`. Данный класс конструируется с помощью метода `SessionRetryContext.create` в который требуется передать реализацию интерфейса `SessionSupplier` - как правило это экземпляр класса `TableClient`.
Дополнительно пользователь может задавать некоторые другие опции
* `maxRetries(int maxRetries)` - максимальное количество повторов операции, не включает в себя первое выполение. Значение по умолчанию `10`
* `retryNotFound(boolean retryNotFound)` - опция повтора операций, вернувших статус `NOT_FOUND`. По умолчанию включено.
* `idempotent(boolean idempotent)` - признак идемпотентности операций. Идемпотентные операции будут повторяться для более широкого списка ошибок. По умолчанию отключено.

Для запуска операций с ретраями класс `SessionRetryContext` предоставляет два метода
* `CompletableFuture<Status> supplyStatus` - выполнение операции, возвращающей статус. В качестве аргумента принимает лямбду `Function<Session, CompletableFuture<Status>> fn`
* `CompletableFuture<Result<T>> supplyResult` - выполнение операции, возвращающей данные. В качестве аргумента принимает лямбду `Function<Session, CompletableFuture<Result<T>>> fn` 

При использовании класса `SessionRetryContext` нужно учитывать, что повторое исполнение операции будут выполнятся в следующих случаях
* Лямбда вернула [retryable](../../../error_handling.md) код ошибки
* В рамках исполнения лямбды была вызвано `UnexpectedResultException` c [retryable](../../../error_handling.md) кодом ошибки



  {% cut "Пример кода, использующего SessionRetryContext.supplyStatus:" %}

    ```java
    private void createTable(TableClient tableClient, String database, String tableName) {
        SessionRetryContext retryCtx = SessionRetryContext.create(tableClient).build();
        TableDescription pets = TableDescription.newBuilder()
                .addNullableColumn("species", PrimitiveType.utf8())
                .addNullableColumn("name", PrimitiveType.utf8())
                .addNullableColumn("color", PrimitiveType.utf8())
                .addNullableColumn("price", PrimitiveType.float32())
                .setPrimaryKeys("species", "name")
                .build();

        String tablePath = database + "/" + tableName;
        retryCtx.supplyStatus(session -> session.createTable(tablePath, pets))
                .join().expect("ok");
    }
    ```

  {% endcut %}

  {% cut "Пример кода, использующего SessionRetryContext.supplyResult:" %}

    ```java
    private void selectData(TableClient tableClient, String tableName) {
        SessionRetryContext retryCtx = SessionRetryContext.create(tableClient).build();
        String selectQuery
                = "DECLARE $species AS Utf8;"
                + "DECLARE $name AS Utf8;"
                + "SELECT * FROM " + tableName + " "
                + "WHERE species = $species AND name = $name;";

        Params params = Params.of(
                "$species", PrimitiveValue.utf8("cat"),
                "$name", PrimitiveValue.utf8("Tom")
        );

        DataQueryResult data = retryCtx
                .supplyResult(session -> session.executeDataQuery(selectQuery, TxControl.onlineRo(), params))
                .join().expect("ok");

        ResultSetReader rsReader = data.getResultSet(0);
        logger.info("Result of select query:");
        while (rsReader.next()) {
            logger.info("  species: {}, name: {}, color: {}, price: {}",
                    rsReader.getColumn("species").getUtf8(),
                    rsReader.getColumn("name").getUtf8(),
                    rsReader.getColumn("color").getUtf8(),
                    rsReader.getColumn("price").getFloat32()
            );
        }
    }
    ```

  {% endcut %}
