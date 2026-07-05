## Как выбрать способ переноса {#how-to-choose}

### Промежуточные файлы (CSV и Parquet) {#staging-files}

[Через промежуточный CSV (YDB CLI)](../../../reference/ydb-cli/export-import/import-file.md) и [Через промежуточный Parquet (YDB CLI)](../../../reference/ydb-cli/export-import/import-file.md) подходят, когда данные сначала выгружаются из источника в файл, а затем загружаются в {{ ydb-short-name }} командой `ydb import file`.

* **CSV** — проще выгрузить из большинства СУБД; удобен для небольших и средних объёмов и ручной отладки.
* **Parquet** — компактнее, быстрее на больших объёмах; типы колонок сохраняются точнее. Рекомендуется, если источник умеет экспортировать Parquet (ClickHouse) или вы уже используете Spark / DuckDB для выгрузки.

Для CSV можно получить черновик DDL по файлу: [{{ ydb-short-name }} CLI tools infer csv](../../../reference/ydb-cli/tools-infer.md).

[ydb-importer](../../../integrations/data-migration/import-jdbc.md) **не читает** промежуточные CSV/Parquet — он подключается к источнику по JDBC и параллельно переносит данные (см. строку ydb-importer в таблице). Если промежуточный файл не нужен, ydb-importer часто удобнее, чем выгрузка в Parquet и обратная загрузка.

### Инструменты, которым нужна готовая таблица в {{ ydb-short-name }} {#existing-table-required}

Следующие способы **не создают** целевую таблицу автоматически — схему нужно подготовить заранее (или сгенерировать для CSV через `tools infer csv`):

* [Через промежуточный CSV (YDB CLI)](../../../reference/ydb-cli/export-import/import-file.md) и [Через промежуточный Parquet (YDB CLI)](../../../reference/ydb-cli/export-import/import-file.md);
* [Федеративные запросы](../../../concepts/query_execution/federated_query/import_and_export.md) — `UPSERT INTO … SELECT … FROM external_table`;
* [Spark](../../../integrations/query-engines/spark.md) — при записи в существующую таблицу (режим append);
* [dbt](../../../integrations/migration/dbt.md) — если модель ссылается на уже созданную целевую таблицу (типичный сценарий — `materialized='incremental'` или DDL вне dbt).

### Инструменты, которые создают таблицы в {{ ydb-short-name }} {#schema-auto}

* [ydb-importer](../../../integrations/data-migration/import-jdbc.md) — строит DDL по метаданным JDBC-источника, опционально пересоздаёт таблицы (`replace-existing`);
* [mysql2ydb](../../../integrations/data-migration/import-mysql.md) — перенос схемы и данных из MySQL;
* [ydb-pg-extension](https://github.com/ydb-platform/ydb-pg-extension/blob/main/docs/migration.md) — создаёт таблицы при миграции из PostgreSQL;
* [dbt](../../../integrations/migration/dbt.md) — при `materialized='table'` выполняет `CREATE TABLE AS` через External Data Source.

### Строковые и колоночные таблицы {#row-vs-column}

| Способ | Строковые таблицы | Колоночные таблицы |
| --- | --- | --- |
| YDB CLI `import file` (CSV/Parquet) | подходит | подходит; для Parquet см. [маппинг типов Arrow/YQL](../../../concepts/query_execution/federated_query/s3/arrow_types_mapping.md) |
| ydb-importer, mysql2ydb, Spark (Bulk Upsert) | подходит | подходит |
| Федеративные запросы `UPSERT` / `REPLACE` | однопоточная запись | **параллельная** запись — предпочтительно для больших объёмов в колоночные таблицы |
| Федеративные запросы `INSERT` | однопоточная запись | параллельная запись |

Для массовой загрузки **колоночных** таблиц из Parquet на S3 также см. [импорт и экспорт колоночных таблиц](../../import-export-column-tables.md) (федеративные запросы или Spark).
