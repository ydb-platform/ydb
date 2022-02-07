# App in Java

This page contains a detailed description of the code of a [test app](https://github.com/yandex-cloud/ydb-java-sdk/tree/master/examples/maven_project) that is available as part of the {{ ydb-short-name }} [Java SDK](https://github.com/yandex-cloud/ydb-java-sdk).

{% include [init.md](steps/01_init.md) %}

App code snippet for driver initialization:

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

App code snippet for creating a session:

```java
        tableClient = tableService.newTableClient();
        session = tableClient.createSession()
            .join()
            .expect("cannot create session");
```

{% include [create_table.md](steps/02_create_table.md) %}

To create tables, use the `Session.CreateTable()` method:

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

You can use the `Session.DescribeTable()` method to output information about the table structure and make sure that it was properly created:

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

To execute YQL queries, use the `Session.executeDataQuery()` method.
The SDK lets you explicitly control the execution of transactions and configure the transaction execution mode using the `TxControl` class.

In the code snippet below, the transaction is executed using the `session.executeDataQuery()` method. The `TxControl txControl = TxControl.serializableRw().setCommitTx(true);` transaction execution mode and `setCommitTx(true)` transaction auto complete flag are set. The query body is described using YQL syntax and is passed to the `executeDataQuery` method as a parameter.

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

When the query is executed, `result.getResultSet(0)` is returned.
The code snippet below shows the output of query results using the `TablePrinter` helper class.

```java
System.out.println("\n--[ SelectSimple ]--");
new TablePrinter(result.getResultSet(0)).print();
```

The given code snippet outputs the following text to the console at startup:

```bash
--[ SelectSimple ]--
+-----------+------------------+--------------------+
| series_id |            title |       release_date |
+-----------+------------------+--------------------+
|   Some[1] | Some["IT Crowd"] | Some["2006-02-03"] |
+-----------+------------------+--------------------+
```

{% include [write_queries.md](steps/05_write_queries.md) %}

Code snippet for inserting and updating data:

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

The code snippet below shows the use of parameterized queries and the `Params` class to generate parameters and pass them to the `executeDataQuery` method.

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

The given code snippet outputs the following text to the console at startup:

```bash
--[ PreparedSelect ]---------------------------------------
+-------------+------------+-----------+-----------+--------------------------------------+
|    air_date | episode_id | season_id | series_id |                                title |
+-------------+------------+-----------+-----------+--------------------------------------+
| Some[16964] |    Some[8] |   Some[3] |   Some[2] | Some["Bachman's Earnings Over-Ride"] |
+-------------+------------+-----------+-----------+--------------------------------------+
```

If there's no prepared query in the session context yet, you can prepare one using `prepareDataQuery`.

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

The first step is to prepare and execute the first query:

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

To continue working within the current transaction, you need to get the current `transaction ID`:

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

The next step is to create the next query that uses the results of code execution on the client side:

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

The given code snippets output the following text to the console at startup:

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

Code snippet for `beginTransaction()` and `transaction.Commit()` calls:

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

