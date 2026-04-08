# Приложение на JavaScript

На этой странице представлено подробное описание кода тестового приложения, использующего [{{ ydb-short-name }} JavaScript SDK](https://github.com/ydb-platform/ydb-js-sdk).

Примеры использования SDK доступны в репозитории [ydb-js-sdk](https://github.com/ydb-platform/ydb-js-sdk/tree/main/examples), а дополнительные примеры реальных сценариев использования — в репозитории [ydb-js-examples](https://github.com/ydb-platform/ydb-js-examples).

{% include [init.md](steps/01_init.md) %}

Для работы с {{ ydb-short-name }} необходимо создать экземпляр драйвера и клиента для выполнения запросов.

Установка необходимых пакетов:

```bash
npm install @ydbjs/core @ydbjs/query
```

Фрагмент кода приложения для инициализации драйвера:

{% list tabs %}

- Используя connectionString

  ```ts
  import { Driver } from '@ydbjs/core'
  import { query } from '@ydbjs/query'

  const connectionString = 'grpc://localhost:2136/local'
  const driver = new Driver(connectionString)
  await driver.ready()

  const sql = query(driver)
  ```

- Используя аутентификацию

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

Фрагмент кода, демонстрирующий выполнение запроса на запись/изменение данных:

```ts
await sql`
    UPSERT INTO episodes (series_id, season_id, episode_id, title)
    VALUES (2, 6, 1, "TBD")
`
```

Для вставки данных с использованием параметров:

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

Для выполнения YQL-запросов используется tagged template синтаксис. Результатом выполнения является массив наборов данных ({{ ydb-short-name }} поддерживает несколько наборов результатов в одном запросе).

```ts
const resultSets = await sql`
    SELECT series_id, title, release_date
    FROM series
    WHERE series_id = 1
`

// resultSets[0] содержит первый набор результатов
const [firstResultSet] = resultSets
console.log(firstResultSet)
// [ { series_id: 1n, title: 'IT Crowd', release_date: 2006-02-03T00:00:00.000Z } ]
```

Для запросов с несколькими наборами результатов:

```ts
type Result = [[{ id: bigint }], [{ count: bigint }]]
const [rows, [{ count }]] = await sql<Result>`
    SELECT series_id as id FROM series;
    SELECT COUNT(*) as count FROM series;
`
```

{% include [param_queries.md](steps/06_param_queries.md) %}

SDK автоматически привязывает параметры через интерполяцию в шаблонных строках. Поддерживаются нативные типы JavaScript, классы значений {{ ydb-short-name }}, массивы и объекты.

```ts
const seriesId = 1n
const title = 'IT Crowd'

const resultSets = await sql`
    SELECT series_id, title, release_date
    FROM series
    WHERE series_id = ${seriesId} AND title = ${title}
`
```

Для именованных параметров и пользовательских типов:

```ts
import { Uint64 } from '@ydbjs/value/primitive'

const id = new Uint64(1n)
const resultSets = await sql`SELECT * FROM series WHERE series_id = $id`.parameter('id', id)
```

Запросы выполняются с потоковой передачей данных по умолчанию. Для работы с большими объёмами данных используйте стандартные запросы:

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

Для выполнения запросов в рамках транзакции используются методы `sql.begin()` или `sql.transaction()`:

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

- begin() с настройками изоляции

  ```ts
  await sql.begin({ isolation: 'snapshotReadOnly', idempotent: true }, async (tx) => {
    return await tx`SELECT COUNT(*) FROM series`
  })
  ```

{% endlist %}

{% include [error-handling.md](steps/50_error_handling.md) %}

Для обработки ошибок используется класс `YDBError`:

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

## Дополнительные возможности

### Настройки запроса

```ts
import { StatsMode } from '@ydbjs/api/query'

await sql`SELECT * FROM series`
  .isolation('onlineReadOnly', { allowInconsistentReads: true })
  .idempotent(true)
  .timeout(5000)
  .withStats(StatsMode.FULL)
```

### Динамические идентификаторы

Для динамических имён таблиц и колонок используйте метод `identifier`:

```ts
const tableName = 'series'
await sql`SELECT * FROM ${sql.identifier(tableName)}`
```

### Закрытие драйвера

Всегда закрывайте драйвер по завершении работы:

```ts
driver.close()
```
