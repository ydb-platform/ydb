# Приложение на Node.js

На этой странице подробно разбирается код [тестового приложения](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/master/examples/basic-example-v1), доступного в составе [Node.js SDK](https://github.com/ydb-platform/ydb-nodejs-sdk) {{ ydb-short-name }}.

{% include [init.md](steps/01_init.md) %}

Фрагмент кода приложения для инициализации драйвера:

```ts
const authService = getCredentialsFromEnv();
logger.debug('Driver initializing...');
const driver = new Driver({endpoint, database, authService});
const timeout = 10000;
if (!await driver.ready(timeout)) {
    logger.fatal(`Driver has not become ready in ${timeout}ms!`);
    process.exit(1);
}
```

Фрагмент кода приложения для создания сессии:

```ts
await driver.tableClient.withSession(async (session) => {
    ...
});
```

{% include [create_table.md](steps/02_create_table.md) %}

Для создания таблиц используется метод `Session.createTable()`:

```ts
async function createTables(session: Session, logger: Logger) {
    logger.info('Creating tables...');
    await session.createTable(
        'series',
        new TableDescription()
            .withColumn(new Column(
                'series_id',
                Types.UINT64,  // not null column
            ))
            .withColumn(new Column(
                'title',
                Types.optional(Types.UTF8),
            ))
            .withColumn(new Column(
                'series_info',
                Types.optional(Types.UTF8),
            ))
            .withColumn(new Column(
                'release_date',
                Types.optional(Types.DATE),
            ))
            .withPrimaryKey('series_id')
    );

    await session.createTable(
        'seasons',
        new TableDescription()
            .withColumn(new Column(
                'series_id',
                Types.optional(Types.UINT64),
            ))
            .withColumn(new Column(
                'season_id',
                Types.optional(Types.UINT64),
            ))
            .withColumn(new Column(
                'title',
                Types.optional(Types.UTF8),
            ))
            .withColumn(new Column(
                'first_aired',
                Types.optional(Types.DATE),
            ))
            .withColumn(new Column(
                'last_aired',
                Types.optional(Types.DATE),
            ))
            .withPrimaryKeys('series_id', 'season_id')
    );

    await session.createTable(
        'episodes',
        new TableDescription()
            .withColumn(new Column(
                'series_id',
                Types.optional(Types.UINT64),
            ))
            .withColumn(new Column(
                'season_id',
                Types.optional(Types.UINT64),
            ))
            .withColumn(new Column(
                'episode_id',
                Types.optional(Types.UINT64),
            ))
            .withColumn(new Column(
                'title',
                Types.optional(Types.UTF8),
            ))
            .withColumn(new Column(
                'air_date',
                Types.optional(Types.DATE),
            ))
            .withPrimaryKeys('series_id', 'season_id', 'episode_id')
    );
}
```

С помощью метода `Session.describeTable()` можно вывести информацию о структуре таблицы и убедиться, что она успешно создалась:

```ts
async function describeTable(session: Session, tableName: string, logger: Logger) {
    const result = await session.describeTable(tableName);
    for (const column of result.columns) {
        logger.info(`Column name '${column.name}' has type ${JSON.stringify(column.type)}`);
    }
}

await describeTable(session, 'series', logger);
await describeTable(session, 'seasons', logger);
await describeTable(session, 'episodes', logger);
```

{% include [steps/03_write_queries.md](steps/03_write_queries.md) %}

Фрагмент кода, демонстрирующий выполнение запроса на запись/изменение данных:

```ts
async function upsertSimple(session: Session, logger: Logger): Promise<void> {
    const query = `
${SYNTAX_V1}
UPSERT INTO episodes (series_id, season_id, episode_id, title) VALUES
(2, 6, 1, "TBD");`;
    logger.info('Making an upsert...');
    await session.executeQuery(query);
    logger.info('Upsert completed');
}
```

{% include [steps/04_query_processing.md](steps/04_query_processing.md) %}

Для выполнения YQL-запросов используется метод `Session.executeQuery()`.

```ts
async function selectSimple(session: Session, logger: Logger): Promise<void> {
    const query = `
${SYNTAX_V1}
SELECT series_id,
       title,
       release_date
FROM series
WHERE series_id = 1;`;
    logger.info('Making a simple select...');
    const {resultSets} = await session.executeQuery(query);
    const result = Series.createNativeObjects(resultSets[0]);
    logger.info(`selectSimple result: ${JSON.stringify(result, null, 2)}`);
}
```

{% include [param_queries.md](steps/06_param_queries.md) %}

Фрагмент кода, приведенный ниже, демонстрирует использование подготовленных с помощью `Session.prepareQuery()` запросов и параметров
в методе `Session.executeQuery()`.

```ts
async function selectPrepared(session: Session, data: ThreeIds[], logger: Logger): Promise<void> {
    const query = `
    ${SYNTAX_V1}
    DECLARE $seriesId AS Uint64;
    DECLARE $seasonId AS Uint64;
    DECLARE $episodeId AS Uint64;

    SELECT title,
           air_date
    FROM episodes
    WHERE series_id = $seriesId AND season_id = $seasonId AND episode_id = $episodeId;`;
    async function select() {
        logger.info('Preparing query...');
        const preparedQuery = await session.prepareQuery(query);
        logger.info('Selecting prepared query...');
        for (const [seriesId, seasonId, episodeId] of data) {
            const episode = new Episode({seriesId, seasonId, episodeId, title: '', airDate: new Date()});
            const {resultSets} = await session.executeQuery(preparedQuery, {
                '$seriesId': episode.getTypedValue('seriesId'),
                '$seasonId': episode.getTypedValue('seasonId'),
                '$episodeId': episode.getTypedValue('episodeId')
            });
            const result = Series.createNativeObjects(resultSets[0]);
            logger.info(`Select prepared query ${JSON.stringify(result, null, 2)}`);
        }
    }
    await withRetries(select);
}
```

{% include [scan-query.md](steps/08_scan_query.md) %}

```ts
async function executeScanQueryWithParams(session: Session, logger: Logger): Promise<void> {
    const query = `
        ${SYNTAX_V1}
        DECLARE $value AS Utf8;

        SELECT key
        FROM ${TABLE}
        WHERE value = $value;`;

    logger.info('Making a stream execute scan query...');

    const params = {
        '$value': TypedValues.utf8('odd'),
    };

    let count = 0;
    await session.streamExecuteScanQuery(query, (result) => {
        logger.info(`Stream scan query partial result #${++count}: ${formatPartialResult(result)}`);
    }, params);

    logger.info(`Stream scan query completed, partial result count: ${count}`);
}
```

{% include [transaction-control.md](steps/10_transaction_control.md) %}

Фрагмент кода, демонстрирующий явное использование вызовов `Session.beginTransaction()` и `Session.сommitTransaction()` для создания и завершения транзакции:

```ts
async function explicitTcl(session: Session, ids: ThreeIds, logger: Logger) {
    const query = `
    ${SYNTAX_V1}
    DECLARE $seriesId AS Uint64;
    DECLARE $seasonId AS Uint64;
    DECLARE $episodeId AS Uint64;

    UPDATE episodes
    SET air_date = CurrentUtcDate()
    WHERE series_id = $seriesId AND season_id = $seasonId AND episode_id = $episodeId;`;
    async function update() {
        logger.info('Running prepared query with explicit transaction control...');
        const preparedQuery = await session.prepareQuery(query);
        const txMeta = await session.beginTransaction({serializableReadWrite: {}});
        const [seriesId, seasonId, episodeId] = ids;
        const episode = new Episode({seriesId, seasonId, episodeId, title: '', airDate: new Date()});
        const params = {
            '$seriesId': episode.getTypedValue('seriesId'),
            '$seasonId': episode.getTypedValue('seasonId'),
            '$episodeId': episode.getTypedValue('episodeId')
        };
        const txId = txMeta.id as string;
        logger.info(`Executing query with txId ${txId}.`);
        await session.executeQuery(preparedQuery, params, {txId});
        await session.commitTransaction({txId});
        logger.info(`TxId ${txId} committed.`);
    }
    await withRetries(update);
}
```

{% include [error-handling.md](steps/50_error_handling.md) %}
