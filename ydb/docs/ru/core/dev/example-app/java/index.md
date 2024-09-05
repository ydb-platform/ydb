# Приложение на Java

На этой странице подробно разбирается код [тестового приложения](https://github.com/ydb-platform/ydb-java-examples/tree/master/basic_example), доступного в составе [Java SDK Examples](https://github.com/ydb-platform/ydb-java-examples) {{ ydb-short-name }}.

## Скачивание SDK Examples и запуск примера {#download}

Приведенный ниже сценарий запуска использует [git](https://git-scm.com/downloads) и [Maven](https://maven.apache.org/download.html). 

Создайте рабочую директорию и выполните в ней из командной строки команду клонирования репозитория с github.com:

``` bash
git clone https://github.com/ydb-platform/ydb-java-examples
```

Далее выполните сборку SDK Examples

``` bash
( cd ydb-java-examples && mvn package )
```

Далее из этой же рабочей директории выполните команду запуска тестового приложения, которая будет отличаться в зависимости от того, к какой базе данных необходимо подключиться.

{% include [run_options.md](_includes/run_options.md) %}


{% include [init.md](../_includes/steps/01_init.md) %}

Основные параметры инициализации драйвера
* Cтрока подключения с информацией об [эндпоинте](../../../concepts/connect.md#endpoint) и [базе данных](../../../concepts/connect.md#database). Единственный обязательный параметр.
* Провайдер [аутенфикации](../../../recipes/ydb-sdk/auth.md##auth-provider). В случае отсутствия прямого указания будет использоваться [анонимное подключение](../../../concepts/auth.md).
* Настройки [пула сессий](../../../recipes/ydb-sdk/session-pool-limit.md)

Фрагмент кода приложения для инициализации драйвера:

```java
this.transport = GrpcTransport.forConnectionString(connectionString)
        .withAuthProvider(CloudAuthHelper.getAuthProviderFromEnviron())
        .build();
this.tableClient = TableClient.newClient(transport).build();
```

Все операции с YDB рекомендуется выполнять с помощью класса-хелпера `SessionRetryContext`, который обеспечивает корректное повторное выполнение операции в случае частичной недоступности. Фрагмент кода для инициализации контекста ретраев:

```java
this.retryCtx = SessionRetryContext.create(tableClient).build();
```

{% include [create_table.md](../_includes/steps/02_create_table.md) %}

Для создания таблиц используется метод `Session.createTable()`:

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

С помощью метода `Session.describeTable()` можно вывести информацию о структуре таблицы и убедиться, что она успешно создалась:

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

Фрагмент кода, демонстрирующий выполнение запроса на запись/изменение данных:

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

Для выполнения YQL-запросов используется метод `Session.executeDataQuery()`.
SDK позволяет в явном виде контролировать выполнение транзакций и настраивать необходимый режим выполнения транзакций с помощью класса `TxControl`.

В фрагменте кода, приведенного ниже, транзакция выполняется с помощью метода `session.executeDataQuery()`. Устанавливается режим выполнения транзакции `TxControl txControl = TxControl.serializableRw().setCommitTx(true);` и флаг автоматического завершения транзакции `setCommitTx(true)`. Тело запроса описано с помощью синтаксиса YQL и как параметр передается методу `executeDataQuery`.

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

В результате исполнения запроса формируется объект класса `DataQueryResult`, который может содержать несколько выборок, получаемых методом `getResultSet( <index> )`. Так как запрос содержал только одну команду `SELECT`, то результат содержит только одну выборку под индексом `0`. Приведенный фрагмент кода при запуске выводит на консоль текст:

```bash
12:06:36.548 INFO  App - --[ SelectSimple ]--
12:06:36.559 INFO  App - read series with id 1, title IT Crowd and release_date 2006-02-03
```

{% include [param_queries.md](../_includes/steps/06_param_queries.md) %}

Фрагмент кода, приведенный ниже, демонстрирует использование параметризованных запросов и класс `Params` для формирования параметров и передачи их методу `executeDataQuery`.

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

Для обеспечения корректности совместной работы транзакций и контекста ретраев каждая транзакция должна выполняться целиком внутри callback, передаваемого в `SessionRetryContext`. Возврат из callback должен происходить после полного завершения транзакции.

Шаблон кода по использованию сложных транзакций в `SessionRetryContext`
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

Первый шаг — подготовка и выполнение первого запроса:

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

Затем мы можем выполнить некоторую клиентскую обработку полученных данных:

```java
    // Perform some client logic on returned values
    ResultSetReader resultSet = res1.getResultSet(0);
    if (!resultSet.next()) {
        throw new RuntimeException("not found first_aired");
    }
    LocalDate fromDate = resultSet.getColumn("from_date").getDate();
    LocalDate toDate = fromDate.plusDays(15);
```

И получить текущий `transaction id` для дальшейшей работы в рамках одной транзакции:

```java
    // Get active transaction id
    String txId = res1.getTxId();
```

Следующий шаг — создание следующего запроса, использующего результаты выполнения кода на стороне клиентского приложения:

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

Приведенные фрагменты кода при запуске выводят на консоль текст:

```bash
12:06:36.850 INFO  App - --[ MultiStep ]--
12:06:36.851 INFO  App - read episode Grow Fast or Die Slow with air date 2018-03-25
12:06:36.851 INFO  App - read episode Reorientation with air date 2018-04-01
12:06:36.851 INFO  App - read episode Chief Operating Officer with air date 2018-04-08
```

{% include [transaction_control.md](../_includes/steps/10_transaction_control.md) %}

Фрагмент кода, демонстрирующий явное использование вызовов `beginTransaction()` и `transaction.commit()`:

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


