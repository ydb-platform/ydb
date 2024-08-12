# Приложение на Node.js

На этой странице подробно разбирается код тестовых приложений 
[basic-example-v2-with-query-service](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/master/examples/basic-example-v2-with-query-service) и 
[basic-example-v1-with-table-service](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/master/examples/basic-example-v1-with-table-service),
доступых в составе [Node.js SDK](https://github.com/ydb-platform/ydb-nodejs-sdk) {{ ydb-short-name }}.

{% include [init.md](steps/01_init.md) %}

Фрагмент кода приложения для инициализации драйвера:

{% list tabs %}
 
- Используя connectionString

  ```ts
  const authService = getCredentialsFromEnv();
  logger.debug('Driver initializing...');
  const driver = new Driver({connectionString, authService});
  const timeout = 10000;
  if (!await driver.ready(timeout)) {
      logger.fatal(`Driver has not become ready in ${timeout}ms!`);
      process.exit(1);
  }
  ```

- Используя endpoint и database

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
    
{% endlist %}

{% note warning %}

Многие операции над YDB могут быть выполнены как через Table Service, так и через Query Service. В документации есть оба варианта.  
При разработке нового ПО рекомендуется использовать Query Service.

{% endnote %}

Фрагмент кода приложения для создания сессии:

{% list tabs %}
    
- Query Service
    
  ```ts
  const result = await driver.queryClient.do({
      ...
      fn: async (session) => {
          ...
      }
  });
  ```
    
- Table Service

  ```ts
  await driver.tableClient.withSession(async (session) => {
      ...
  });
  ```
    
{% endlist %}

{% include [create_table.md](steps/02_create_table.md) %}

{% list tabs %}

- Query Service
    
  ```ts
  async function createTables(driver: Driver, logger: Logger) {
      logger.info('Dropping old tables and create new ones...');
      await driver.queryClient.do({
          fn: async (session) => {
              await session.execute({
                  text: `
                      DROP TABLE IF EXISTS ${SERIES_TABLE};
                      DROP TABLE IF EXISTS ${EPISODES_TABLE};
                      DROP TABLE IF EXISTS ${SEASONS_TABLE};
  
                      CREATE TABLE ${SERIES_TABLE}
                      (
                          series_id    UInt64,
                          title        Utf8,
                          series_info  Utf8,
                          release_date DATE,
                          PRIMARY KEY (series_id)
                      );
  
                      CREATE TABLE ${SEASONS_TABLE}
                      (
                          series_id   UInt64,
                          season_id   UInt64,
                          title UTF8,
                          first_aired DATE,
                          last_aired DATE,
                          PRIMARY KEY (series_id, season_id)
                      );
  
                      CREATE TABLE ${EPISODES_TABLE}
                      (
                          series_id  UInt64,
                          season_id  UInt64,
                          episode_id UInt64,
                          title      UTf8,
                          air_date   DATE,
                          PRIMARY KEY (series_id, season_id, episode_id),
                          INDEX      episodes_index GLOBAL ASYNC ON (air_date)
                      );`,
              });
          },
      });
  }
  ```

- Table Service

  Для создания таблиц используется метод `TableSession.createTable()`:
  
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
    
{% endlist %}

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

_Прим.:_ Query Service не поддерживает функциональность подобную `Session.describeTable()`.

{% endlist %}

{% include [steps/03_write_queries.md](steps/03_write_queries.md) %}

Фрагмент кода, демонстрирующий выполнение запроса на запись/изменение данных:

{% list tabs %}

- Query Service

  ```ts
  async function upsertSimple(driver: Driver, logger: Logger): Promise<void> {
      logger.info('Making an upsert...');
      await driver.queryClient.do({
          fn: async (session) => {
               await session.execute({
                   text: `
                      UPSERT INTO ${EPISODES_TABLE} (series_id, season_id, episode_id, title)
                      VALUES (2, 6, 1, "TBD");`,
             })
          }
      });
      logger.info('Upsert completed.')
  }
  ```

- Table Service

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
{% endlist %}

{% include [steps/04_query_processing.md](steps/04_query_processing.md) %}

{% list tabs %}

- Query Service

  {% list tabs %}
  
  - rowMode: RowType.Native
  
    Для выполнения YQL-запросов используется метод `QuerySession.execute()`.
      
    В зависимости оп параметра rowMode данные можно получить в javascript форме или как YDB структуры.  
  
    ```ts
    async function selectNativeSimple(driver: Driver, logger: Logger): Promise<void> {
        logger.info('Making a simple native select...');
        const result = await driver.queryClient.do({
            fn: async (session) => {
                const {resultSets} =
                    await session.execute({
                        // rowMode: RowType.Native, // Result set cols and rows returned as native javascript values. It's default behaviour
                        text: `
                            SELECT series_id,
                                   title,
                                   release_date
                            FROM ${SERIES_TABLE}
                            WHERE series_id = 1;`,
                    });
                const {value: resultSet1} = await resultSets.next();
                const rows: any[][] = []
                for await (const row of resultSet1.rows) rows.push(row);
                return {cols: resultSet1.columns, rows};
            }
        });
        logger.info(`selectNativeSimple cols: ${JSON.stringify(result.cols, null, 2)}`);
        logger.info(`selectNativeSimple rows: ${JSON.stringify(result.rows, null, 2)}`);
    }
    ```
  
  - rowMode: RowType.Ydb
  
      Для выполнения YQL-запросов используется метод `QuerySession.execute()`.
      
      В зависимости оп параметра rowMode данные можно получить в javascript форме иил как YDB структуры.  
      
      ```ts
      async function selectTypedSimple(driver: Driver, logger: Logger): Promise<void> {
          logger.info('Making a simple typed select...');
          const result = await driver.queryClient.do({
              fn: async (session) => {
                  const {resultSets} =
                      await session.execute({
                          rowMode: RowType.Ydb, // enables typedRows() on result sets
                          text: `
                              SELECT series_id,
                                     title,
                                     release_date
                              FROM ${SERIES_TABLE}
                              WHERE series_id = 1;`,
                      });
                  const {value: resultSet1} = await resultSets.next();
                  const rows: Series[] = [];
                  // Note: resultSet1.rows will iterate YDB IValue structures
                  for await (const row of resultSet1.typedRows(Series)) rows.push(row);
                  return {cols: resultSet1.columns, rows};
              }
          });
          logger.info(`selectTypedSimple cols: ${JSON.stringify(result.cols, null, 2)}`);
          logger.info(`selectTypedSimple rows: ${JSON.stringify(result.rows, null, 2)}`);
      }
      ```
  
  {% endlist %}

- Table Service
  
  Для выполнения YQL-запросов используется метод `TableSession.executeQuery()`.
  
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

{% endlist %}

{% include [param_queries.md](steps/06_param_queries.md) %}

{% list tabs %}

- Query Service

  В Query Service нет явной опции Prepared Query.  YDB определяет необходимость использование этого режима самостоятельно.
  
  ```ts
  async function selectWithParameters(driver: Driver, data: ThreeIds[], logger: Logger): Promise<void> {
       
      await driver.queryClient.do({
          fn: async (session) => {
              for (const [seriesId, seasonId, episodeId] of data) {
                  const episode = new Episode({seriesId, seasonId, episodeId, title: '', airDate: new Date()});
                  const {resultSets, opFinished} = await session.execute({
                      parameters: {
                          '$seriesId': episode.getTypedValue('seriesId'),
                          '$seasonId': episode.getTypedValue('seasonId'),
                          '$episodeId': episode.getTypedValue('episodeId')
                      },
                      text: `
                          SELECT title,
                                 air_date
                          FROM episodes
                          WHERE series_id = $seriesId
                            AND season_id = $seasonId
                            AND episode_id = $episodeId;`
                  });
                  const {value: resultSet} = await resultSets.next();
                  const {value: row} = await resultSet.rows.next();
                  await opFinished;
                  logger.info(`Select prepared query ${JSON.stringify(row, null, 2)}`);
              }
          }
      });
  }
  ```

- Table Service

  Фрагмент кода, приведенный ниже, демонстрирует использование подготовленных с помощью `Session.prepareQuery()` запросов и параметров
  в методе `TableSession.executeQuery()`.
  
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

{% endlist %}

{% include [scan-query.md](steps/08_scan_query.md) %}

{% list tabs %}

- Query Service
    
  В Query Service для получения данных потоком используется метод `QuerySession.execute()`.
  
  ```ts
  async function selectWithParametrs(driver: Driver, data: ThreeIds[], logger: Logger): Promise<void> {
      logger.info('Selecting prepared query...');
      await driver.queryClient.do({
          fn: async (session) => {
              for (const [seriesId, seasonId, episodeId] of data) {
                  const episode = new Episode({seriesId, seasonId, episodeId, title: '', airDate: new Date()});
  
                  // Note: In query service execute() there is no "prepared query" option.
                  //       This behaviour applied by YDB according to an internal rule
  
                  const {resultSets, opFinished} = await session.execute({
                      parameters: {
                          '$seriesId': episode.getTypedValue('seriesId'),
                          '$seasonId': episode.getTypedValue('seasonId'),
                          '$episodeId': episode.getTypedValue('episodeId')
                      },
                      text: `
                          SELECT title,
                                 air_date
                          FROM episodes
                          WHERE series_id = $seriesId
                            AND season_id = $seasonId
                            AND episode_id = $episodeId;`
                  });
                  const {value: resultSet} = await resultSets.next();
                  const {value: row} = await resultSet.rows.next();
                  await opFinished;
                  logger.info(`Select prepared query ${JSON.stringify(row, null, 2)}`);
              }
          }
      });
  }
  ```

- Table Service

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

{% endlist %}

{% include [transaction-control.md](steps/10_transaction_control.md) %}

Фрагмент кода, демонстрирующий явное использование вызовов `Session.beginTransaction()` и `Session.сommitTransaction()` для создания и завершения транзакции:

{% list tabs %}

- Query Service

  {% list tabs %}

  - do()
      
    ```ts
    async function explicitTcl(driver: Driver, ids: ThreeIds, logger: Logger) {
        logger.info('Running prepared query with explicit transaction control...');
        await driver.queryClient.do({
            fn: async (session) => {
                await session.beginTransaction({serializableReadWrite: {}});
                const [seriesId, seasonId, episodeId] = ids;
                const episode = new Episode({seriesId, seasonId, episodeId, title: '', airDate: new Date()});
                await session.execute({
                    parameters: {
                        '$seriesId': episode.getTypedValue('seriesId'),
                        '$seasonId': episode.getTypedValue('seasonId'),
                        '$episodeId': episode.getTypedValue('episodeId')
                    },
                    text: `
                        UPDATE episodes
                        SET air_date = CurrentUtcDate()
                        WHERE series_id = $seriesId
                          AND season_id = $seasonId
                          AND episode_id = $episodeId;`
                })
                const txId = session.txId;
                await session.commitTransaction();
                logger.info(`TxId ${txId} committed.`);
            }
        });
    }
    ```
  
  - doTx()
  
    ```ts
    async function transactionPerWholeDo(driver: Driver, ids: ThreeIds, logger: Logger) {
        logger.info('Running query with one transaction per whole doTx()...');
        await driver.queryClient.doTx({
            txSettings: {serializableReadWrite: {}},
            fn: async (session) => {
                const [seriesId, seasonId, episodeId] = ids;
                const episode = new Episode({seriesId, seasonId, episodeId, title: '', airDate: new Date()});
                await session.execute({
                    parameters: {
                        '$seriesId': episode.getTypedValue('seriesId'),
                        '$seasonId': episode.getTypedValue('seasonId'),
                        '$episodeId': episode.getTypedValue('episodeId')
                    },
                    text: `
                        UPDATE episodes
                        SET air_date = CurrentUtcDate()
                        WHERE series_id = $seriesId
                          AND season_id = $seasonId
                          AND episode_id = $episodeId;`
                })
                logger.info(`TxId ${session.txId} will be committed by doTx().`);
            }
        });
    }
    ```

  {% endlist %}

- Table Service

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

{% endlist %}

{% include [error-handling.md](steps/50_error_handling.md) %}
