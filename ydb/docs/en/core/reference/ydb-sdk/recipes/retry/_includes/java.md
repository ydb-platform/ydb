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
                  .addNullableColumn("species", PrimitiveType.Text)
                  .addNullableColumn("name", PrimitiveType.Text)
                  .addNullableColumn("color", PrimitiveType.Text)
                  .addNullableColumn("price", PrimitiveType.Float)
                  .setPrimaryKeys("species", "name")
                  .build();

          String tablePath = database + "/" + tableName;
          retryCtx.supplyStatus(session -> session.createTable(tablePath, pets))
                  .join().expectSuccess();
      }
    ```

  {% endcut %}

  {% cut "Snippet of code using SessionRetryContext.supplyResult:" %}

    ```java
      private void selectData(TableClient tableClient, String tableName) {
          SessionRetryContext retryCtx = SessionRetryContext.create(tableClient).build();
          String selectQuery
                  = "DECLARE $species AS Text;"
                  + "DECLARE $name AS Text;"
                  + "SELECT * FROM " + tableName + " "
                  + "WHERE species = $species AND name = $name;";

          Params params = Params.of(
                  "$species", PrimitiveValue.newText("cat"),
                  "$name", PrimitiveValue.newText("Tom")
          );

          DataQueryResult data = retryCtx
                  .supplyResult(session -> session.executeDataQuery(selectQuery, TxControl.onlineRo(), params))
                  .join().getValue();

          ResultSetReader rsReader = data.getResultSet(0);
          logger.info("Result of select query:");
          while (rsReader.next()) {
              logger.info("  species: {}, name: {}, color: {}, price: {}",
                      rsReader.getColumn("species").getText(),
                      rsReader.getColumn("name").getText(),
                      rsReader.getColumn("color").getText(),
                      rsReader.getColumn("price").getFloat()
              );
          }
      }
    ```

  {% endcut %}

