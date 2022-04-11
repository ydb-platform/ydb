In the {{ ydb-short-name }} Java SDK, the request retry mechanism is implemented as the `com.yandex.ydb.table.SessionRetryContext` helper class. This class is built using the `SessionRetryContext.create` method, where you should pass the implementation of the `SessionSupplier` interface (usually, this is an instance of the `TableClient` class).
Additionally, the user can set some other options.

* `maxRetries(int maxRetries)`: The maximum number of operation retries, excluding the first execution. Defaults to `10`.
* `retryNotFound(boolean retryNotFound)`: The option to retry operations that return the `NOT_FOUND` status. Enabled by default.
* `idempotent(boolean idempotent)`: Indicates if an operation is idempotent. The system will retry idempotent operations for a wider list of errors. Disabled by default.

The `SessionRetryContext` class provides two methods to run operations with retries.

* `CompletableFuture<Status> supplyStatus`: Run an operation that returns a status. Takes the `Function<Session, CompletableFuture<Status>> fn` lambda as an argument.
* `CompletableFuture<Result<T>> supplyResult`: Run an operation that returns data. Takes the `Function<Session, CompletableFutureResult<T>> fn` lambda as an argument.

When using the `SessionRetryContext` class, keep in mind that operation retries will be made in the following cases:

* The lamda function returns the [retryable](../../../error_handling.md) error code.

* While executing the lambda function, the `UnexpectedResultException` with the [retryable](../../../error_handling.md) error code is raised.

  {% cut "Snippet of code using SessionRetryContext.supplyStatus:" %}

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

  {% cut "Snippet of code using SessionRetryContext.supplyResult:" %}

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

