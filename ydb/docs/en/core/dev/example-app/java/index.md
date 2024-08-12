# App in Java

This page contains a detailed description of the code of a [test app](https://github.com/ydb-platform/ydb-java-examples/tree/master/basic_example) that is available as part of the {{ ydb-short-name }} [Java SDK Examples](https://github.com/ydb-platform/ydb-java-examples).

## Downloading SDK Examples and running the example {#download}

The following execution scenario is based on [Git](https://git-scm.com/downloads) and [Maven](https://maven.apache.org/download.html).

Create a working directory and use it to run from the command line the command to clone the GitHub repository:

```bash
git clone https://github.com/ydb-platform/ydb-java-examples
```

Then build the SDK Examples

```bash
( cd ydb-java-examples && mvn package )
```

Next, from the same working directory, run the command to start the test app. The command will differ depending on the database to connect to.

{% include [run_options.md](_includes/run_options.md) %}


{% include [init.md](../_includes/steps/01_init.md) %}

Main driver initialization parameters
* A connection string containing details about an [endpoint](../../../concepts/connect.md#endpoint) and [database](../../../concepts/connect.md#database). This is the only parameter that is required.
* [Authentication](../../../recipes/ydb-sdk/auth.md#auth-provider) provider. Unless explicitly specified, an [anonymous connection](../../../concepts/auth.md) is used.
* [Session pool](../../../recipes/ydb-sdk/session-pool-limit.md) settings

App code snippet for driver initialization:

```java
this.transport = GrpcTransport.forConnectionString(connectionString)
        .withAuthProvider(CloudAuthHelper.getAuthProviderFromEnviron())
        .build();
this.tableClient = TableClient.newClient(transport).build();
```

We recommend that you use the `SessionRetryContext` helper class for all your operations with the YDB: it ensures proper retries in case the database becomes partially unavailable. Sample code to initialize the retry context:

```java
this.retryCtx = SessionRetryContext.create(tableClient).build();
```

{% include [create_table.md](../_includes/steps/02_create_table.md) %}

To create tables, use the `Session.createTable()` method:

```java
private void createTables() {
    TableDescription seriesTable = TableDescription.newBuilder()
        .addNullableColumn("series_id", PrimitiveType.Uint64)
        .addNullableColumn("title", PrimitiveType.Text)
        .addNullableColumn("series_info", PrimitiveType.Text)
        .addNullableColumn("release_date", PrimitiveType.Date)
        .setPrimaryKey("series_id")
        .build();

    retryCtx.supplyStatus(session -> session.createTable(database + "/series", seriesTable))
            .join().expectSuccess("Can't create table /series");

    TableDescription seasonsTable = TableDescription.newBuilder()
        .addNullableColumn("series_id", PrimitiveType.Uint64)
        .addNullableColumn("season_id", PrimitiveType.Uint64)
        .addNullableColumn("title", PrimitiveType.Text)
        .addNullableColumn("first_aired", PrimitiveType.Date)
        .addNullableColumn("last_aired", PrimitiveType.Date)
        .setPrimaryKeys("series_id", "season_id")
        .build();

    retryCtx.supplyStatus(session -> session.createTable(database + "/seasons", seasonsTable))
            .join().expectSuccess("Can't create table /seasons");

    TableDescription episodesTable = TableDescription.newBuilder()
        .addNullableColumn("series_id", PrimitiveType.Uint64)
        .addNullableColumn("season_id", PrimitiveType.Uint64)
        .addNullableColumn("episode_id", PrimitiveType.Uint64)
        .addNullableColumn("title", PrimitiveType.Text)
        .addNullableColumn("air_date", PrimitiveType.Date)
        .setPrimaryKeys("series_id", "season_id", "episode_id")
        .build();

    retryCtx.supplyStatus(session -> session.createTable(database + "/episodes", episodesTable))
            .join().expectSuccess("Can't create table /episodes");
}
```

You can use the `Session.describeTable()` method to view information about the table structure and make sure that it was properly created:

```java
private void describeTables() {
    logger.info("--[ DescribeTables ]--");

    Arrays.asList("series", "seasons", "episodes").forEach(tableName -> {
        String tablePath = database + '/' + tableName;
        TableDescription tableDesc = retryCtx.supplyResult(session -> session.describeTable(tablePath))
                .join().getValue();

        List<String> primaryKeys = tableDesc.getPrimaryKeys();
        logger.info(" table {}", tableName);
        for (TableColumn column : tableDesc.getColumns()) {
            boolean isPrimary = primaryKeys.contains(column.getName());
            logger.info("     {}: {} {}", column.getName(), column.getType(), isPrimary ? " (PK)" : "");
        }
    });
}
```
{% include [../steps/03_write_queries.md](../_includes/steps/03_write_queries.md) %}

Code snippet for data insert/update:

```java
private void upsertSimple() {
    String query
            = "UPSERT INTO episodes (series_id, season_id, episode_id, title) "
            + "VALUES (2, 6, 1, \"TBD\");";

    // Begin new transaction with SerializableRW mode
    TxControl txControl = TxControl.serializableRw().setCommitTx(true);

    // Executes data query with specified transaction control settings.
    retryCtx.supplyResult(session -> session.executeDataQuery(query, txControl))
        .join().getValue();
}
```

{% include [steps/04_query_processing.md](../_includes/steps/04_query_processing.md) %}

To execute YQL queries, use the `Session.executeDataQuery()` method.
The SDK lets you explicitly control the execution of transactions and configure the transaction execution mode using the `TxControl` class.

In the code snippet below, the transaction is executed using the `session.executeDataQuery()` method. The `TxControl txControl = TxControl.serializableRw().setCommitTx(true);` transaction execution mode and `setCommitTx(true)` transaction auto complete flag are set. The query body is described using YQL syntax and is passed to the `executeDataQuery` method as a parameter.

```java
private void selectSimple() {
    String query
            = "SELECT series_id, title, release_date "
            + "FROM series WHERE series_id = 1;";

    // Begin new transaction with SerializableRW mode
    TxControl txControl = TxControl.serializableRw().setCommitTx(true);

    // Executes data query with specified transaction control settings.
    DataQueryResult result = retryCtx.supplyResult(session -> session.executeDataQuery(query, txControl))
            .join().getValue();

    logger.info("--[ SelectSimple ]--");

    ResultSetReader rs = result.getResultSet(0);
    while (rs.next()) {
        logger.info("read series with id {}, title {} and release_date {}",
                rs.getColumn("series_id").getUint64(),
                rs.getColumn("title").getText(),
                rs.getColumn("release_date").getDate()
        );
    }
}
```

As a result of the query, an object of the `DataQueryResult` class is generated. It may contain several sets obtained using the `getResultSet( <index> )` method. Since there was only one `SELECT` statement in the query, the result contains only one selection indexed as `0`. The given code snippet prints the following text to the console at startup:

```bash
12:06:36.548 INFO  App - --[ SelectSimple ]--
12:06:36.559 INFO  App - read series with id 1, title IT Crowd and release_date 2006-02-03
```

{% include [param_queries.md](../_includes/steps/06_param_queries.md) %}

The code snippet below shows the use of parameterized queries and the `Params` class to generate parameters and pass them to the `executeDataQuery` method.

```java
private void selectWithParams(long seriesID, long seasonID) {
    String query
            = "DECLARE $seriesId AS Uint64; "
            + "DECLARE $seasonId AS Uint64; "
            + "SELECT sa.title AS season_title, sr.title AS series_title "
            + "FROM seasons AS sa INNER JOIN series AS sr ON sa.series_id = sr.series_id "
            + "WHERE sa.series_id = $seriesId AND sa.season_id = $seasonId";

    // Begin new transaction with SerializableRW mode
    TxControl txControl = TxControl.serializableRw().setCommitTx(true);

    // Type of parameter values should be exactly the same as in DECLARE statements.
    Params params = Params.of(
            "$seriesId", PrimitiveValue.newUint64(seriesID),
            "$seasonId", PrimitiveValue.newUint64(seasonID)
    );

    DataQueryResult result = retryCtx.supplyResult(session -> session.executeDataQuery(query, txControl, params))
            .join().getValue();

    logger.info("--[ SelectWithParams ] -- ");

    ResultSetReader rs = result.getResultSet(0);
    while (rs.next()) {
        logger.info("read season with title {} for series {}",
                rs.getColumn("season_title").getText(),
                rs.getColumn("series_title").getText()
        );
    }
}
```

{% include [scan_query.md](../_includes/steps/08_scan_query.md) %}

```java
private void scanQueryWithParams(long seriesID, long seasonID) {
    String query
            = "DECLARE $seriesId AS Uint64; "
            + "DECLARE $seasonId AS Uint64; "
            + "SELECT ep.title AS episode_title, sa.title AS season_title, sr.title AS series_title "
            + "FROM episodes AS ep "
            + "JOIN seasons AS sa ON sa.season_id = ep.season_id "
            + "JOIN series AS sr ON sr.series_id = sa.series_id "
            + "WHERE sa.series_id = $seriesId AND sa.season_id = $seasonId;";

    // Type of parameter values should be exactly the same as in DECLARE statements.
    Params params = Params.of(
            "$seriesId", PrimitiveValue.newUint64(seriesID),
            "$seasonId", PrimitiveValue.newUint64(seasonID)
    );

    logger.info("--[ ExecuteScanQueryWithParams ]--");
    retryCtx.supplyStatus(session -> {
        ExecuteScanQuerySettings settings = ExecuteScanQuerySettings.newBuilder().build();
        return session.executeScanQuery(query, params, settings, rs -> {
            while (rs.next()) {
                logger.info("read episode {} of {} for {}",
                        rs.getColumn("episode_title").getText(),
                        rs.getColumn("season_title").getText(),
                        rs.getColumn("series_title").getText()
                );
            }
        });
    }).join().expectSuccess("scan query problem");
}
```

{% include [multistep_transactions.md](../_includes/steps/09_multistep_transactions.md) %}

To ensure interoperability between the transactions and the retry context, each transaction must wholly execute inside the callback passed to `SessionRetryContext`. The callback must return after the entire transaction is completed.

Code template for running complex transactions inside `SessionRetryContext`
```java
private void multiStepTransaction(long seriesID, long seasonID) {
    retryCtx.supplyStatus(session -> {
        // Multiple operations with session
        ...

        // return success status to SessionRetryContext
        return CompletableFuture.completedFuture(Status.SUCCESS);
    }).join().expectSuccess("multistep transaction problem");
}

```

The first step is to prepare and execute the first query:

```java
    String query1
            = "DECLARE $seriesId AS Uint64; "
            + "DECLARE $seasonId AS Uint64; "
            + "SELECT MIN(first_aired) AS from_date FROM seasons "
            + "WHERE series_id = $seriesId AND season_id = $seasonId;";

    // Execute first query to get the required values to the client.
    // Transaction control settings don't set CommitTx flag to keep transaction active
    // after query execution.
    TxControl tx1 = TxControl.serializableRw().setCommitTx(false);
    DataQueryResult res1 = session.executeDataQuery(query1, tx1, Params.of(
            "$seriesId", PrimitiveValue.newUint64(seriesID),
            "$seasonId", PrimitiveValue.newUint64(seasonID)
    )).join().getValue();
```

After that, we can process the resulting data on the client side:

```java
    // Perform some client logic on returned values
    ResultSetReader resultSet = res1.getResultSet(0);
    if (!resultSet.next()) {
        throw new RuntimeException("not found first_aired");
    }
    LocalDate fromDate = resultSet.getColumn("from_date").getDate();
    LocalDate toDate = fromDate.plusDays(15);
```

And get the current `transaction id` to continue processing within the same transaction:

```java
    // Get active transaction id
    String txId = res1.getTxId();
```

The next step is to create the next query that uses the results of code execution on the client side:

```java
    // Construct next query based on the results of client logic
    String query2
            = "DECLARE $seriesId AS Uint64;"
            + "DECLARE $fromDate AS Date;"
            + "DECLARE $toDate AS Date;"
            + "SELECT season_id, episode_id, title, air_date FROM episodes "
            + "WHERE series_id = $seriesId AND air_date >= $fromDate AND air_date <= $toDate;";

    // Execute second query.
    // Transaction control settings continues active transaction (tx) and
    // commits it at the end of second query execution.
    TxControl tx2 = TxControl.id(txId).setCommitTx(true);
    DataQueryResult res2 = session.executeDataQuery(query2, tx2, Params.of(
        "$seriesId", PrimitiveValue.newUint64(seriesID),
        "$fromDate", PrimitiveValue.newDate(fromDate),
        "$toDate", PrimitiveValue.newDate(toDate)
    )).join().getValue();

    logger.info("--[ MultiStep ]--");
    ResultSetReader rs = res2.getResultSet(0);
    while (rs.next()) {
        logger.info("read episode {} with air date {}",
                rs.getColumn("title").getText(),
                rs.getColumn("air_date").getDate()
        );
    }
```

The given code snippets output the following text to the console at startup:

```bash
12:06:36.850 INFO  App - --[ MultiStep ]--
12:06:36.851 INFO  App - read episode Grow Fast or Die Slow with air date 2018-03-25
12:06:36.851 INFO  App - read episode Reorientation with air date 2018-04-01
12:06:36.851 INFO  App - read episode Chief Operating Officer with air date 2018-04-08
```

{% include [transaction_control.md](../_includes/steps/10_transaction_control.md) %}

Code snippet for `beginTransaction()` and `transaction.commit()` calls:

```java
private void tclTransaction() {
    retryCtx.supplyStatus(session -> {
        Transaction transaction = session.beginTransaction(Transaction.Mode.SERIALIZABLE_READ_WRITE)
            .join().getValue();

        String query
                = "DECLARE $airDate AS Date; "
                + "UPDATE episodes SET air_date = $airDate WHERE title = \"TBD\";";

        Params params = Params.of("$airDate", PrimitiveValue.newDate(Instant.now()));

        // Execute data query.
        // Transaction control settings continues active transaction (tx)
        TxControl txControl = TxControl.id(transaction).setCommitTx(false);
        DataQueryResult result = session.executeDataQuery(query, txControl, params)
            .join().getValue();

        logger.info("get transaction {}", result.getTxId());

        // Commit active transaction (tx)
        return transaction.commit();
    }).join().expectSuccess("tcl transaction problem");
}
```


