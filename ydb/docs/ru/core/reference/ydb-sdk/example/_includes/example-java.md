# Приложение на Java

На этой странице подробно разбирается код [тестового приложения](https://github.com/yandex-cloud/ydb-java-sdk/tree/master/examples/maven_project), доступного в составе [Java SDK](https://github.com/yandex-cloud/ydb-java-sdk) {{ ydb-short-name }}.

{% include [init.md](steps/01_init.md) %}

Фрагмент кода приложения для инициализации драйвера:

```java
        RpcTransport transport = GrpcTransport.forEndpoint(args.endpoint, args.database)
            .withAuthProvider(new TokenAuthProvider(ydbToken))
            .build();

        try (TableService tableService = TableServiceBuilder.ownTransport(transport).build()) {
            String path = args.path == null ? args.database : args.path;
            try (App example = appFactory.newApp(tableService, path)) {
                example.run();
            } catch (Throwable t) {
                t.printStackTrace();
                System.exit(1);
            }
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(1);
        }
```

Фрагмент кода приложения для создания сессии:

```java
        tableClient = tableService.newTableClient();
        session = tableClient.createSession()
            .join()
            .expect("cannot create session");
```

{% include [create_table.md](steps/02_create_table.md) %}

Для создания таблиц используется метод `Session.createTable()`:

```java
private void createTables() {
    TableDescription seriesTable = TableDescription.newBuilder()
        .addNullableColumn("series_id", PrimitiveType.uint64())
        .addNullableColumn("title", PrimitiveType.utf8())
        .addNullableColumn("series_info", PrimitiveType.utf8())
        .addNullableColumn("release_date", PrimitiveType.uint64())
        .setPrimaryKey("series_id")
        .build();

    execute(session -> session.createTable(database + "/series", seriesTable).join());

    TableDescription seasonsTable = TableDescription.newBuilder()
        .addNullableColumn("series_id", PrimitiveType.uint64())
        .addNullableColumn("season_id", PrimitiveType.uint64())
        .addNullableColumn("title", PrimitiveType.utf8())
        .addNullableColumn("first_aired", PrimitiveType.uint64())
        .addNullableColumn("last_aired", PrimitiveType.uint64())
        .setPrimaryKeys("series_id", "season_id")
        .build();

    execute(session -> session.createTable(database + "/seasons", seasonsTable).join());

    TableDescription episodesTable = TableDescription.newBuilder()
        .addNullableColumn("series_id", PrimitiveType.uint64())
        .addNullableColumn("season_id", PrimitiveType.uint64())
        .addNullableColumn("episode_id", PrimitiveType.uint64())
        .addNullableColumn("title", PrimitiveType.utf8())
        .addNullableColumn("air_date", PrimitiveType.uint64())
        .setPrimaryKeys("series_id", "season_id", "episode_id")
        .build();

    execute(session -> session.createTable(database + "/episodes", episodesTable).join());
}
```

С помощью метода `Session.describeTable()` можно вывести информацию о структуре таблицы и убедиться, что она успешно создалась:

```java
private void describeTables() {
    System.out.println("\n--[ DescribeTables ]--");

    for (String tableName : new String[]{ "series", "seasons", "episodes" }) {
        String tablePath = database + '/' + tableName;
        TableDescription tableDesc = executeWithResult(session -> session.describeTable(tablePath).join());

        System.out.println(tablePath + ':');
        List<String> primaryKeys = tableDesc.getPrimaryKeys();
        for (TableColumn column : tableDesc.getColumns()) {
            boolean isPrimary = primaryKeys.contains(column.getName());
            System.out.println("    " + column.getName() + ": " + column.getType() + (isPrimary ? " (PK)" : ""));
        }
        System.out.println();
    }
}
```

{% include [pragmatablepathprefix.md](auxilary/pragmatablepathprefix.md) %}

{% include [query_processing.md](steps/03_query_processing.md) %}

Для выполнения YQL-запросов используется метод `Session.executeDataQuery()`.
SDK позволяет в явном виде контролировать выполнение транзакций и настраивать необходимый режим выполнения транзакций с помощью класса `TxControl`.

В фрагменте кода, приведенного ниже, транзакция выполняется с помощью метода `session.executeDataQuery()`. Устанавливается режим выполнения транзакции `TxControl txControl = TxControl.serializableRw().setCommitTx(true);` и флаг автоматического завершения транзакции `setCommitTx(true)`. Тело запроса описано с помощью синтаксиса YQL и как параметр передается методу `executeDataQuery`.

```java
private void selectSimple() {
    String query = String.format(
        "PRAGMA TablePathPrefix(\"%s\");\n" +
        "$format = DateTime::Format(\"%%Y-%%m-%%d\");\n" +
        "\n" +
        "SELECT\n" +
        "    series_id,\n" +
        "    title,\n" +
        "    $format(DateTime::FromSeconds(CAST(DateTime::ToSeconds(DateTime::IntervalFromDays(CAST(release_date AS Int16))) AS Uint32))) AS release_date\n" +
        "FROM series\n" +
        "WHERE series_id = 1;",
        database);

    TxControl txControl = TxControl.serializableRw().setCommitTx(true);

    DataQueryResult result = executeWithResult(session -> session.executeDataQuery(query, txControl).join());

    System.out.println("\n--[ SelectSimple ]--");
    new TablePrinter(result.getResultSet(0)).print();
}
```

{% include [results_processing.md](steps/04_results_processing.md) %}

В качестве результатов выполнения запроса возвращается `result.getResultSet(0)`.
Фрагмент кода, приведенный ниже, демонстрирует вывод результатов запроса с помощью вспомогательного класса `TablePrinter`.

```java
System.out.println("\n--[ SelectSimple ]--");
new TablePrinter(result.getResultSet(0)).print();
```

Приведенный фрагмент кода при запуске выводит на консоль текст:

```bash
--[ SelectSimple ]--
+-----------+------------------+--------------------+
| series_id |            title |       release_date |
+-----------+------------------+--------------------+
|   Some[1] | Some["IT Crowd"] | Some["2006-02-03"] |
+-----------+------------------+--------------------+
```

{% include [write_queries.md](steps/05_write_queries.md) %}

Фрагмент кода, демонстрирующий выполнение запроса на запись/изменение данных:

```java
private void upsertSimple() {
    String query = String.format(
        "PRAGMA TablePathPrefix(\"%s\");\n" +
        "\n" +
        "UPSERT INTO episodes (series_id, season_id, episode_id, title) VALUES\n" +
        "(2, 6, 1, \"TBD\");",
        database);

    TxControl txControl = TxControl.serializableRw().setCommitTx(true);

    execute(session -> session.executeDataQuery(query, txControl)
        .join()
        .toStatus());
}
```

{% include [write_queries.md](steps/05_write_queries.md) %}

Фрагмент кода, приведенный ниже, демонстрирует использование параметризованных запросов и класс `Params` для формирования параметров и передачи их методу `executeDataQuery`.

```java
private void preparedSelect(long seriesId, long seasonId, long episodeId) {
    final String queryId = "PreparedSelectTransaction";

    DataQuery query = preparedQueries.get(queryId);
    if (query == null) {
        String queryText = String.format(
            "PRAGMA TablePathPrefix(\"%s\");\n" +
            "\n" +
            "DECLARE $seriesId AS Uint64;\n" +
            "DECLARE $seasonId AS Uint64;\n" +
            "DECLARE $episodeId AS Uint64;\n" +
            "\n" +
            "SELECT *\n" +
            "FROM episodes\n" +
            "WHERE series_id = $seriesId AND season_id = $seasonId AND episode_id = $episodeId;",
            database);

        query = executeWithResult(session -> session.prepareDataQuery(queryText).join());
        System.out.println("Finished preparing query: " + queryId);

        preparedQueries.put(queryId, query);
    }

    Params params = query.newParams()
        .put("$seriesId", uint64(seriesId))
        .put("$seasonId", uint64(seasonId))
        .put("$episodeId", uint64(episodeId));

    TxControl txControl = TxControl.serializableRw().setCommitTx(true);
    DataQueryResult result = query.execute(txControl, params)
        .join()
        .expect("prepared query failed");

    System.out.println("\n--[ PreparedSelect ]--");
    new TablePrinter(result.getResultSet(0)).print();
}
```

{% include [param_prep_queries.md](steps/07_param_prep_queries.md) %}

```java
    /**
     * Shows usage of prepared queries.
     */
    private void preparedSelect(long seriesId, long seasonId, long episodeId) {
        final String queryId = "PreparedSelectTransaction";

        // Once prepared, query data is stored in the session and identified by QueryId.
        // We keep a track of prepared queries available in current session to reuse them in
        // consecutive calls.

        PreparedQuery query = preparedQueries.get(queryId);
        if (query == null) {
            String queryText = String.format(
                "PRAGMA TablePathPrefix(\"%s\");\n" +
                "\n" +
                "DECLARE $seriesId AS Uint64;\n" +
                "DECLARE $seasonId AS Uint64;\n" +
                "DECLARE $episodeId AS Uint64;\n" +
                "\n" +
                "SELECT *\n" +
                "FROM episodes\n" +
                "WHERE series_id = $seriesId AND season_id = $seasonId AND episode_id = $episodeId;",
                path);

            // Prepares query and stores its QueryId for the current session.
            query = executeWithResult(session -> session.prepareDataQuery(queryText).join());
            System.out.println("Finished preparing query: " + queryId);

            preparedQueries.put(queryId, query);
        }

        Params params = query.newParams()
            .put("$seriesId", DataValue.uint64(seriesId))
            .put("$seasonId", DataValue.uint64(seasonId))
            .put("$episodeId", DataValue.uint64(episodeId));

        DataQueryResult result = query.execute(TxControl.serializableRw().setCommitTx(true), params)
            .join()
            .expect("prepared query failed");

        System.out.println("\n--[ PreparedSelect ]---------------------------------------");
        new TablePrinter(result.getResultSet(0)).print();
    }
```

Приведенный фрагмент кода при запуске выводит на консоль текст:

```bash
--[ PreparedSelect ]---------------------------------------
+-------------+------------+-----------+-----------+--------------------------------------+
|    air_date | episode_id | season_id | series_id |                                title |
+-------------+------------+-----------+-----------+--------------------------------------+
| Some[16964] |    Some[8] |   Some[3] |   Some[2] | Some["Bachman's Earnings Over-Ride"] |
+-------------+------------+-----------+-----------+--------------------------------------+
```

Если подготовленного запроса в контексте сессии еще не существует, его можно подготовить с помощью `prepareDataQuery`.

{% include [scan_query.md](steps/08_scan_query.md) %}

```java
private void executeScanQuery(Session session) {
    String query =
        "SELECT series_id, season_id, COUNT(*) AS episodes_count\n" +
        "FROM episodes\n" +
        "GROUP BY series_id, season_id\n" +
        "ORDER BY series_id, season_id;";

    ExecuteScanQuerySettings settings = ExecuteScanQuerySettings.newBuilder().build();
    Consumer<ResultSetReader> printer = (ResultSetReader reader) -> {
        while (reader.next()) {
            for (int i = 0; i < reader.getColumnCount(); i++) {
                if (i > 0) {
                    System.out.print(" ");
                }
                System.out.print(reader.getColumn(i).toString());
            }
            System.out.print("\n");
        }
    };

    session.executeScanQuery(query, Params.empty(), settings, printer).join();
}
```

{% include [multistep_transactions.md](steps/09_multistep_transactions.md) %}

Первый шаг — подготовка и выполнение первого запроса:

```java
public void multiStep() {
    final long seriesId = 2;
    final long seasonId = 5;

    final String txId;
    final Instant fromDate;
    final Instant toDate;

    {
        String query = String.format(
            "PRAGMA TablePathPrefix(\"%s\");\n" +
            "\n" +
            "DECLARE $seriesId AS Uint64;\n" +
            "DECLARE $seasonId AS Uint64;\n" +
            "\n" +
            "SELECT first_aired AS from_date FROM seasons\n" +
            "WHERE series_id = $seriesId AND season_id = $seasonId;",
            path);

        Params params = Params.withUnknownTypes()
            .put("$seriesId", DataType.uint64(), DataValue.uint64(seriesId))
            .put("$seasonId", DataType.uint64(), DataValue.uint64(seasonId));

        // Executes the first query to get the required values to the client.
        // Transaction control settings don't set CommitTx flag to keep transaction active
        // after query execution.
        TxControl txControl = TxControl.serializableRw();
        DataQueryResult result = executeWithResult(session -> session.executeDataQuery(query, txControl, params)
            .join());

        if (result.isEmpty()) {
            throw new IllegalStateException("empty result set");
        }
```

Для продолжения работы в рамках текущей транзакции необходимо получить текущий `transaction id`:

```java
    ResultSetReader resultSet = result.getResultSet(0);
    resultSet.next();
    long firstAired = resultSet.getColumn(0).getUint64();

    // Performs some client logic on returned values.
    fromDate = Instant.EPOCH.plus(firstAired, ChronoUnit.DAYS);
    toDate = fromDate.plus(15, ChronoUnit.DAYS);

    // Gets active transaction id.
    txId = result.getTxId();
}
```

Следующий шаг — создание следующего запроса, использующего результаты выполнения кода на стороне клиентского приложения:

```java
    {
        // Constructs next query based on the results of client logic.
        String query = String.format(
            "PRAGMA TablePathPrefix(\"%s\");\n" +
            "\n" +
            "DECLARE $seriesId AS Uint64;\n" +
            "DECLARE $fromDate AS Uint64;\n" +
            "DECLARE $toDate AS Uint64;\n" +
            "\n" +
            "SELECT season_id, episode_id, title, air_date FROM episodes\n" +
            "WHERE series_id = $seriesId AND air_date >= $fromDate AND air_date <= $toDate;",
            path);

        Params params = Params.withUnknownTypes()
            .put("$seriesId", DataType.uint64(), DataValue.uint64(seriesId))
            .put("$fromDate", DataType.uint64(), DataValue.uint64(Duration.between(Instant.EPOCH, fromDate).toDays()))
            .put("$toDate", DataType.uint64(), DataValue.uint64(Duration.between(Instant.EPOCH, toDate).toDays()));

        // Executes second query.
        // Transaction control settings continues active transaction (tx) and
        // commits it at the end of second query execution.
        TxControl txControl = TxControl.id(txId).setCommitTx(true);
        DataQueryResult result = executeWithResult(session -> session.executeDataQuery(query, txControl, params)
            .join());

        System.out.println("\n--[ MultiStep ]---------------------------------------");
        // Index of result set corresponds to its order in YQL query.
        new TablePrinter(result.getResultSet(0)).print();
    }
}
```

Приведенные фрагменты кода при запуске выводят на консоль текст:

```bash
--[ MultiStep ]---------------------------------------
+-----------+------------+---------------------------------+-------------+
| season_id | episode_id |                           title |    air_date |
+-----------+------------+---------------------------------+-------------+
|   Some[5] |    Some[1] |   Some["Grow Fast or Die Slow"] | Some[17615] |
|   Some[5] |    Some[2] |           Some["Reorientation"] | Some[17622] |
|   Some[5] |    Some[3] | Some["Chief Operating Officer"] | Some[17629] |
+-----------+------------+---------------------------------+-------------+
```



{% include [transaction_control.md](steps/10_transaction_control.md) %}

Фрагмент кода, демонстрирующий явное использование вызовов `beginTransaction()` и `transaction.Commit()`:

```java
private Status explicitTcl(Session session) {
    Result<Transaction> transactionResult = session.beginTransaction(TransactionMode.SERIALIZABLE_READ_WRITE)
        .join();
    if (!transactionResult.isSuccess()) {
        return transactionResult.toStatus();
    }

    Transaction transaction = transactionResult.expect("cannot begin transaction");
    String query = String.format(
        "PRAGMA TablePathPrefix(\"%s\");\n" +
        "DECLARE $airDate AS Uint64;\n" +
        "UPDATE episodes SET air_date = $airDate WHERE title = \"TBD\";",
        database);

    Params params = Params.of("$airDate", uint64(Duration.between(Instant.EPOCH, Instant.now()).toDays()));

    TxControl txControl = TxControl.id(transaction).setCommitTx(false);
    Result<DataQueryResult> updateResult = session.executeDataQuery(query, txControl, params)
        .join();
    if (!updateResult.isSuccess()) {
        return updateResult.toStatus();
    }

    return transaction.commit().join();
}
```

{% include [error_handling.md](steps/50_error_handling.md) %}
