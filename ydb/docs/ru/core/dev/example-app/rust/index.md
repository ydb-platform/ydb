# Пример приложения в Rust

<!-- markdownlint-disable blanks-around-fences -->

На этой странице описан [базовый пример Query Service](https://github.com/ydb-platform/ydb-rs-sdk/tree/master/ydb/examples/basic_query_series) из [Rust SDK](https://github.com/ydb-platform/ydb-rs-sdk) {{ ydb-short-name }} (crate `ydb` на crates.io). Пример соответствует Go-версии `basic/native/query`: создаёт таблицы `series`, `seasons` и `episodes`, загружает тестовые данные о сериалах и читает метаданные через Query Service.

## Получение и запуск {#download}

Нужны [Git](https://git-scm.com/downloads) и [Rust](https://www.rust-lang.org/tools/install) 1.85+. Установка SDK — в [документации](../../../reference/ydb-sdk/install.md).

Клонируйте репозиторий:

```bash
git clone https://github.com/ydb-platform/ydb-rs-sdk.git
```

Запуск примера:

{% include [run_options.md](_includes/run_options.md) %}

{% include [init.md](../_includes/steps/01_init.md) %}

Импорт и инициализация клиента:

```rust
use ydb::{ClientBuilder, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let connection_string = std::env::var("YDB_CONNECTION_STRING")
        .unwrap_or_else(|_| "grpc://localhost:2136?database=local".to_string());

    let client = ClientBuilder::new_from_connection_string(connection_string)?.client()?;
    client.wait().await?;

    let mut qc = client.query_client().clone_with_idempotent_operations(true);
    // ...
    Ok(())
}
```

`ClientBuilder::new_from_connection_string` принимает строку подключения (`grpc://host:port?database=/path`). `client.wait()` ждёт discovery эндпоинтов. `query_client()` — вход в API Query Service.

Для локального Docker по умолчанию используется анонимная аутентификация. Для токена — [`ClientBuilder::with_credentials`](https://docs.rs/ydb/latest/ydb/struct.ClientBuilder.html), см. [рецепты аутентификации](../../../recipes/ydb-sdk/auth.md).

## Клиент Query Service {#query-client}

Одноразовые запросы — awaitable builders на [`QueryClient`](https://docs.rs/ydb/latest/ydb/struct.QueryClient.html):

- `qc.exec(yql)` — без результирующего набора (DDL, DML).
- `qc.query_row(yql)` — одна строка.
- `qc.query(yql).await?` — потоковый [`QueryStream`](https://docs.rs/ydb/latest/ydb/struct.QueryStream.html).

Повторы при ошибках: `clone_with_idempotent_operations(true)` для идемпотентных чтений и DDL.

{% include [steps/02_create_table.md](../_includes/steps/02_create_table.md) %}

Создание таблицы (implicit session, DDL без явного tx_control):

```rust
qc.exec(format!(
    "CREATE TABLE IF NOT EXISTS `{}` (
        series_id Bytes,
        title Utf8,
        series_info Utf8,
        release_date Date,
        comment Utf8,
        PRIMARY KEY(series_id)
    )",
    "native/query/series"
))
.await?;
```

{% include [steps/03_write_queries.md](../_includes/steps/03_write_queries.md) %}

Пакетная загрузка через `AS_TABLE`:

```rust
use ydb::{Value, ydb_struct};

let rows: Vec<Value> = /* ... */;
let list = Value::list_from(example_row, rows)?;
qc.exec("DECLARE $seriesData AS List<Struct<...>>; REPLACE INTO ... FROM AS_TABLE($seriesData);")
    .param("$seriesData", list)
    .await?;
```

{% include [steps/04_query_processing.md](../_includes/steps/04_query_processing.md) %}

Потоковое чтение в режиме snapshot read-only:

```rust
use ydb::QueryTxMode;

let mut stream = qc
    .query("SELECT series_id, title, release_date FROM `native/query/series`")
    .with_tx_mode(QueryTxMode::SnapshotReadOnly)
    .idempotent(true)
    .await?;

while let Some(result_set) = stream.next_result_set().await? {
    for mut row in result_set {
        // извлечение колонок из row
    }
}
stream.close().await?;
```

{% include [steps/06_param_queries.md](../_includes/steps/06_param_queries.md) %}

Параметры: `.param(name, value)` или макрос `ydb_params!`.

{% include [steps/10_transaction_control.md](../_includes/steps/10_transaction_control.md) %}

Явные режимы изоляции: `.with_tx_mode(QueryTxMode::SnapshotReadOnly)` (one-shot) или [`QueryTransactionOptions`](https://docs.rs/ydb/latest/ydb/struct.QueryTransactionOptions.html) для интерактивных транзакций. По умолчанию one-shot — implicit: сервер выбирает изоляцию из SQL.
