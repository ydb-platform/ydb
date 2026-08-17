# Приложение на JavaScript

На этой странице представлено подробное описание кода [тестового приложения](https://github.com/ydb-platform/ydb-js-sdk/tree/main/examples/query),
доступного в составе [{{ ydb-short-name }} JavaScript SDK](https://github.com/ydb-platform/ydb-js-sdk).

{% include [init.md](steps/01_init.md) %}

Фрагмент кода приложения для инициализации драйвера:

```js
import { Driver } from '@ydbjs/core';
import { query } from '@ydbjs/query';

const connectionString = process.env.YDB_CONNECTION_STRING || 'grpc://localhost:2136/local';
const driver = new Driver(connectionString);
const sql = query(driver);

await driver.ready();
```

{% include [create_table.md](steps/02_create_table.md) %}

```js
await sql`CREATE TABLE IF NOT EXISTS series (
    series_id   Uint64,
    title       Text,
    series_info Text,
    release_date Date,
    PRIMARY KEY (series_id)
)`;

await sql`CREATE TABLE IF NOT EXISTS seasons (
    series_id  Uint64,
    season_id  Uint64,
    title      Text,
    first_aired Date,
    last_aired  Date,
    PRIMARY KEY (series_id, season_id)
)`;

await sql`CREATE TABLE IF NOT EXISTS episodes (
    series_id  Uint64,
    season_id  Uint64,
    episode_id Uint64,
    title      Text,
    air_date   Date,
    PRIMARY KEY (series_id, season_id, episode_id)
)`;
```

{% include [steps/03_write_queries.md](steps/03_write_queries.md) %}

Фрагмент кода, демонстрирующий выполнение запроса на запись данных через `AS_TABLE`:

```js
import { Uint64, TypedDate } from '@ydbjs/value/primitive';

const seriesData = [
    {
        series_id: new Uint64(1n),
        title: 'IT Crowd',
        series_info: 'The IT Crowd is a British sitcom...',
        release_date: new TypedDate(new Date('2006-02-03')),
    },
    {
        series_id: new Uint64(2n),
        title: 'Silicon Valley',
        series_info: 'Silicon Valley is an American comedy...',
        release_date: new TypedDate(new Date('2014-04-06')),
    },
];

await sql`INSERT INTO series SELECT * FROM AS_TABLE(${seriesData})`;
```

{% include [steps/04_query_processing.md](steps/04_query_processing.md) %}

Для выполнения YQL-запросов используется тег-шаблон `sql`. Параметры передаются через интерполяцию:

```js
const seriesId = 1n;
const [[series]] = await sql`
    SELECT series_id, title, release_date
    FROM series
    WHERE series_id = ${seriesId}`;

console.log('Series:', series);
```

Для получения нескольких строк:

```js
const rows = await sql`
    SELECT series_id, season_id, title
    FROM seasons
    WHERE series_id = ${seriesId}`;

for (const [row] of rows) {
    console.log(row);
}
```

{% include [param_queries.md](steps/06_param_queries.md) %}

Параметры передаются прямо в шаблонный литерал — SDK автоматически типизирует их:

```js
const targetSeriesId = 2n;
const targetSeasonId = 1n;

const episodes = await sql`
    SELECT episode_id, title, air_date
    FROM episodes
    WHERE series_id = ${targetSeriesId}
      AND season_id = ${targetSeasonId}`;

for (const [episode] of episodes) {
    console.log(episode);
}
```

{% include [transaction-control.md](steps/10_transaction_control.md) %}

Фрагмент кода, демонстрирующий выполнение транзакции:

```js
await sql.begin(async (tx) => {
    const [[episode]] = await tx`
        SELECT title, air_date
        FROM episodes
        WHERE series_id = ${1n} AND season_id = ${1n} AND episode_id = ${1n}`;

    await tx`
        UPDATE episodes
        SET air_date = CurrentUtcDate()
        WHERE series_id = ${1n} AND season_id = ${1n} AND episode_id = ${1n}`;
});
```

{% include [error-handling.md](steps/50_error_handling.md) %}
