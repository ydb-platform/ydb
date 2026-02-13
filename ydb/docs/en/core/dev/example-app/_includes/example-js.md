# Example app in JavaScript

This page provides a detailed description of the example application code that uses the [{{ ydb-short-name }} JavaScript SDK](https://github.com/ydb-platform/ydb-js-sdk).

Usage examples are available in the [ydb-js-sdk](https://github.com/ydb-platform/ydb-js-sdk/tree/main/examples) repository, and additional real-world scenario examples can be found in the [ydb-js-examples](https://github.com/ydb-platform/ydb-js-examples) repository.

{% include [init.md](steps/01_init.md) %}

To work with {{ ydb-short-name }}, you need to create a driver instance and a query client.

Installing the required packages:

```bash
npm install @ydbjs/core @ydbjs/query
```

App code snippet for driver initialization:

{% list tabs %}

- Using connectionString

  ```ts
  import { Driver } from '@ydbjs/core'
  import { query } from '@ydbjs/query'

  const connectionString = 'grpc://localhost:2136/local'
  const driver = new Driver(connectionString)
  await driver.ready()

  const sql = query(driver)
  ```

- Using authentication

  ```ts
  import { Driver } from '@ydbjs/core'
  import { query } from '@ydbjs/query'
  import { AnonymousCredentialsProvider } from '@ydbjs/auth'

  const connectionString = 'grpc://localhost:2136/local'
  const driver = new Driver(connectionString, {
    credentialsProvider: new AnonymousCredentialsProvider(),
  })
  await driver.ready()

  const sql = query(driver)
  ```

{% endlist %}

{% include [create_table.md](steps/02_create_table.md) %}

  ```ts
  await sql`
      CREATE TABLE series (
          series_id Uint64,
          title Text,
          series_info Text,
          release_date Date,
          PRIMARY KEY (series_id)
      )
  `

  await sql`
      CREATE TABLE seasons (
          series_id Uint64,
          season_id Uint64,
          title Text,
          first_aired Date,
          last_aired Date,
          PRIMARY KEY (series_id, season_id)
      )
  `

  await sql`
      CREATE TABLE episodes (
          series_id Uint64,
          season_id Uint64,
          episode_id Uint64,
          title Text,
          air_date Date,
          PRIMARY KEY (series_id, season_id, episode_id)
      )
  `
  ```

{% include [steps/03_write_queries.md](steps/03_write_queries.md) %}

Code snippet for data insert/update:

```ts
await sql`
    UPSERT INTO episodes (series_id, season_id, episode_id, title)
    VALUES (2, 6, 1, "TBD")
`
```

For inserting data using parameters:

```ts
import { Uint64, Text, Date as YdbDate } from '@ydbjs/value/primitive'

const data = [
  {
    series_id: new Uint64(1n),
    title: new Text('IT Crowd'),
    series_info: new Text('British sitcom'),
    release_date: new YdbDate(new Date('2006-02-03')),
  },
]

await sql`INSERT INTO series SELECT * FROM AS_TABLE(${data})`
```

{% include [steps/04_query_processing.md](steps/04_query_processing.md) %}

The tagged template syntax is used for executing YQL queries. The result is an array of result sets ({{ ydb-short-name }} supports multiple result sets per query).

```ts
const resultSets = await sql`
    SELECT series_id, title, release_date
    FROM series
    WHERE series_id = 1
`

// resultSets[0] contains the first result set
const [firstResultSet] = resultSets
console.log(firstResultSet)
// [ { series_id: 1n, title: 'IT Crowd', release_date: 2006-02-03T00:00:00.000Z } ]
```

For queries with multiple result sets:

```ts
type Result = [[{ id: bigint }], [{ count: bigint }]]
const [rows, [{ count }]] = await sql<Result>`
    SELECT series_id as id FROM series;
    SELECT COUNT(*) as count FROM series;
`
```

{% include [param_queries.md](steps/06_param_queries.md) %}

The SDK automatically binds parameters through interpolation in template strings. Native JavaScript types, {{ ydb-short-name }} value classes, arrays, and objects are all supported.

```ts
const seriesId = 1n
const title = 'IT Crowd'

const resultSets = await sql`
    SELECT series_id, title, release_date
    FROM series
    WHERE series_id = ${seriesId} AND title = ${title}
`
```

For named parameters and custom types:

```ts
import { Uint64 } from '@ydbjs/value/primitive'

const id = new Uint64(1n)
const resultSets = await sql`SELECT * FROM series WHERE series_id = $id`.parameter('id', id)
```

{% include [scan-query.md](steps/08_scan_query.md) %}

Queries are executed with streaming data transfer by default. For working with large volumes of data, use standard queries:

```ts
const resultSets = await sql`
    SELECT series_id, season_id, title, first_aired
    FROM seasons
    WHERE series_id IN (1, 2)
    ORDER BY season_id
`

for (const row of resultSets[0]) {
  console.log(`Season ${row.season_id}: ${row.title}`)
}
```

{% include [transaction-control.md](steps/10_transaction_control.md) %}

To execute queries within a transaction, use the `sql.begin()` or `sql.transaction()` methods:

{% list tabs %}

- begin() — serializable read-write

  ```ts
  const result = await sql.begin(async (tx) => {
    await tx`
        UPDATE episodes
        SET air_date = CurrentUtcDate()
        WHERE series_id = 2 AND season_id = 6 AND episode_id = 1
    `
    return await tx`SELECT * FROM episodes WHERE series_id = 2`
  })
  ```

- begin() with isolation settings

  ```ts
  await sql.begin({ isolation: 'snapshotReadOnly', idempotent: true }, async (tx) => {
    return await tx`SELECT COUNT(*) FROM series`
  })
  ```

{% endlist %}

{% include [error-handling.md](steps/50_error_handling.md) %}

The `YDBError` class is used for error handling:

```ts
import { YDBError } from '@ydbjs/error'

try {
  await sql`SELECT * FROM non_existent_table`
} catch (error) {
  if (error instanceof YDBError) {
    console.error('YDB Error:', error.message)
  }
}
```

## Additional features

### Query options

```ts
import { StatsMode } from '@ydbjs/api/query'

await sql`SELECT * FROM series`
  .isolation('onlineReadOnly', { allowInconsistentReads: true })
  .idempotent(true)
  .timeout(5000)
  .withStats(StatsMode.FULL)
```

### Dynamic identifiers

For dynamic table and column names, use the `identifier` method:

```ts
const tableName = 'series'
await sql`SELECT * FROM ${sql.identifier(tableName)}`
```

### Closing the driver

Always close the driver when finished:

```ts
driver.close()
```
