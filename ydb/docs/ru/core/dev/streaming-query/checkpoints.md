# Отключение чекпоинтов

Для снижения накладных расходов (от сохранения чекпоинтов) возможно отключение [чекпоинтов](../../concepts/glossary.md#checkpoints). Для этого используется прагма `ydb.DisableCheckpoints`. При этом нужно учитывать отсутствие гарантий на консистентность данных (при пользовательских или внутренних перезапусках запроса). Используйте исключительно с целью отладки.

```yql
CREATE STREAMING QUERY query_without_checkpoints AS
DO BEGIN

PRAGMA ydb.DisableCheckpoints = "TRUE";

INSERT INTO
    ydb_source.output_topic
SELECT
    *
FROM
    ydb_source.input_topic

END DO
```
