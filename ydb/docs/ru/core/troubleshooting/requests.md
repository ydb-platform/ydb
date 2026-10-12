# Диагностика запросов

Логирование позволяет получить подробный лог исполняемых запросов.

{% include [log_sink_columnshard](../../../_includes/log_sink_columnshard.md) %}

Возможна выгрузка логов запросов в локальную базу данных или какое-либо другое хранилище информации (см. [настройки логирования](../../../reference/configuration/log_config.md#events) ). Структура формируемой таблицы:

| Поле | Тип |Описание |
|---|---|---|
| `req_id` | `utf8` | Идентификатор запроса |
| `pool_id` | `utf8` | Идентификатор пула |
| `session_id` | `utf8` | Идентификатор сессии |
| `user_sid` | `utf8` | SID пользователя |
| `request` | `utf8` | Текст запроса |
| `issues` | `utf8` | Описание результата исполнения запроса (в т.ч. информация об ошибке) |
| `is_streaming_query` | `utf8` | Признак потокового запроса |
| `duration_us` | `utf8` | Время исполнения |
| `database` | `utf8` | Название базы данных |
| `database_id` | `utf8` | Идентификатор базы данных |
| `trace_id` | `utf8` | Идентификатор трассировки |
| `query_id` | `utf8` | Идентификатор компиляции запроса |
| `action` | `utf8` | Действие. Возможные значения: `QUERY_ACTION_EXECUTE`, `QUERY_ACTION_EXPLAIN`, `QUERY_ACTION_PREPARE`, `QUERY_ACTION_EXECUTE_PREPARED` |
| `type` | `utf8` | Тип запроса. Возможные значения: `QUERY_TYPE_SQL_DML`, `QUERY_TYPE_SQL_DDL`, `QUERY_TYPE_SQL_SCAN` |
| `started_at_us` | `utf8` | Время начала исполнения запроса |
| `status` | `utf8` | Статус исполнения |
| `queued_time_us` | `utf8` | Время от начала обработки запроса в Session Actor до ответа workload manager |
| `compile_from_cache` | `utf8` | Признак, что результат компиляции запроса был взят из кэша |
| `compile_time_us` | `utf8` | Время компиляции запроса |


Кроме того, таблица имеет [набор стандартных полей](../core/reference/configuration/log_config.md#event-local-database-columns).

