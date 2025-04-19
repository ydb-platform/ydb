# Example app in Node.js

This page provides a detailed description of the code for
[an example application](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/master/examples/basic-example-v2-with-query-service),
which is available as part of the [{{ ydb-short-name }} Node.js SDK](https://github.com/ydb-platform/ydb-nodejs-sdk).

{% include [init.md](steps/01_init.md) %}

App code snippet for driver initialization:

{% list tabs %}

- Using connectionString

  ```ts
  const authService = getCredentialsFromEnv();
  logger.debug('Driver initializing...');
  const driver = new Driver({connectionString, authService});
  const timeout = 10000;
  if (!await driver.ready(timeout)) {
      logger.fatal(`Driver did not become ready within ${timeout}ms!`);
      process.exit(1);
  }
  ```

- Using endpoint and database

  ```ts
  const authService = getCredentialsFromEnv();
  logger.debug('Driver initializing...');
  const driver = new Driver({endpoint, database, authService});
  const timeout = 10000;
  if (!await driver.ready(timeout)) {
      logger.fatal(`Driver did not become ready within ${timeout}ms!`);
      process.exit(1);
  }
  ```

{% endlist %}

App code snippet for creating a session:

  ```ts
  const result = await driver.queryClient.do({
      ...
      fn: async (session) => {
          ...
      }
  });
  ```

{% include [create_table.md](steps/02_create_table.md) %}

  ```ts
  async function createTables(driver: Driver, logger: Logger) {
      logger.info('Dropping old tables and creating new ones...');
      await driver.queryClient.do({
          fn: async (session) => {

            try {
                await session.execute({
                    text: `
                        DROP TABLE ${SERIES_TABLE};
                        DROP TABLE ${EPISODES_TABLE};
                        DROP TABLE ${SEASONS_TABLE};`,
                });
            } catch (err) { // Ignore if tables are missing
                if (err instanceof SchemeError) throw err;
            }

            await session.execute({
                text: `
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

{% include [steps/03_write_queries.md](steps/03_write_queries.md) %}

Code snippet for data insert/update:

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

{% include [steps/04_query_processing.md](steps/04_query_processing.md) %}

The `QuerySession.execute()` method is used to execute YQL queries.

Depending on the rowMode parameter, the data can be retrieved in javascript form or as YDB structures.

{% list tabs %}

- rowMode: RowType.Native

  ```ts
  async function selectNativeSimple(driver: Driver, logger: Logger): Promise<void> {
      logger.info('Making a simple native select...');
      const result = await driver.queryClient.do({
          fn: async (session) => {
              const {resultSets} =
                  await session.execute({
                      // rowMode: RowType.Native, // Result set columns and rows are returned as native JavaScript values. This is the default behavior.
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

{% include [param_queries.md](steps/06_param_queries.md) %}


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
                        DECLARE $seriesId AS Uint64;
                        DECLARE $seasonId AS Uint64;
                        DECLARE $episodeId AS Uint64;

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
                logger.info(`Parametrized select query ${JSON.stringify(row, null, 2)}`);
            }
        }
    });
}
```

{% include [scan-query.md](steps/08_scan_query.md) %}

The `QuerySession.execute()` method is used to retrieve data in a streaming manner.

```ts
async function selectWithParametrs(driver: Driver, data: ThreeIds[], logger: Logger): Promise<void> {
    logger.info('Selecting with a parametrized query...');
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
                        DECLARE $seriesId AS Uint64;
                        DECLARE $seasonId AS Uint64;
                        DECLARE $episodeId AS Uint64;

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
                logger.info(`Parametrized select query ${JSON.stringify(row, null, 2)}`);
            }
        }
    });
}
```

{% include [transaction-control.md](steps/10_transaction_control.md) %}

Here's a code sample demonstrating how to explicitly use the `Session.beginTransaction()` and `Session.commitTransaction()` calls to execute a transaction:

{% list tabs %}

- do()

  ```ts
  async function explicitTcl(driver: Driver, ids: ThreeIds, logger: Logger) {
      logger.info('Running a parametrized query with explicit transaction control...');
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
                      DECLARE $seriesId AS Uint64;
                      DECLARE $seasonId AS Uint64;
                      DECLARE $episodeId AS Uint64;
 
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
      logger.info('Running a query with one transaction per whole doTx()...');
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
                      DECLARE $seriesId AS Uint64;
                      DECLARE $seasonId AS Uint64;
                      DECLARE $episodeId AS Uint64;

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

{% include [error-handling.md](steps/50_error_handling.md) %}
