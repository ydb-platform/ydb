# Аудитное логирование DML-операций

Записи о DML-операциях позволяют наблюдать за доступом пользователей к данным в таблицах.

{% note info "" %}

DML ([Data Manipulation Language](https://en.wikipedia.org/wiki/Data_manipulation_language)) &mdash; подмножество инструкций SQL, которые изменяют данные в таблицах.

{% endnote %}

## Атрибуты записей

| __Атрибут__ | __Описание__ |
|:----|:----|
| `component` | Логирующий компонент, всегда `grpc-proxy`.
| `remote_address` | IP-адрес клиента, приславшего запрос.
| `subject` | User SID пользователя, от имени которого выполняется операция.
| `database` | Путь базы данных, в которой выполняется операция.
| `operation`| Тип запроса.
| `start_time` | Время начала операции.
| `end_time` | Время окончания операции.
| `status` | Общий статус операции: `SUCCESS` или `ERROR`.
| `detailed_status` | Детальный статус операции.

### Атрибуты по типам запросов

| __Тип__[**](*popup-note) | __Атрибут__ | __Описание__ |
|:----|:----|:----|
| _PrepareDataQuery_ | `query_text` | Текст запроса.
|| `prepared_query_id` | Идентификатор подготовленного запроса.
| _BeginTransaction<br>CommitTransaction<br>RollbackTransaction_ | `tx_id` | Уникальный идентификатор транзакции.
| _ExecuteDataQuery_ | `query_text` | Текст запроса.<br>В записи присутствует либо `query_text`, либо `prepared_query_id`.
|| `prepared_query_id` | Идентификатор ранее подготовленного запроса.<br>В записи присутствует либо `query_text`, либо `prepared_query_id`.
|| `tx_id` | Уникальный идентификатор транзакции.
|| `begin_tx` | Запрос на неявный старт транзакции.
|| `commit_tx` | Запрос на неявный commit транзакции по завершении запроса.
| _ExecuteYql<br>ExecuteScript_  | `query_text` | Текст запроса.
| _ExecuteQuery_ | `query_text` | Текст запроса.
|| `tx_id` | Уникальный идентификатор транзакции.
|| `begin_tx` | Запрос на неявный старт транзакции.
|| `commit_tx` | Запрос на неявный commit транзакции по завершении запроса.
| _BulkUpsert_ | `table` | Путь изменяемой таблицы.
|| `row_count` | Количество изменённых строк.

## Включение и настройка

Аудитное логирование должно быть [включено](audit-log.md#enabling-audit-log) на кластере.

Дополнительно, логирование DML-операций требует включения на уровне базы данных, его нельзя включить глобально или на отдельной таблице.

У каждой базы есть:
- `EnableDmlAudit` &mdash; флаг включения логирования (значение по-умолчанию: _false_)
- `ExpectedSubjects` &mdash; список пользовательских SID, действия из-под которых ожидаемы, не представляют интереса, и не должны попадать в аудитный лог (значение по-умолчанию: _пустой_)

Настройки являются частью схемы базы данных:
- Устанавливаются и меняются DDL операциями создания и изменения базы данных.
- Текущее состояние настроек показывается в ответе на [describe](../reference/ydb-cli/commands/scheme-describe.md) базы.
    <br>Например:
    ```yaml
    PathDescription:
        DomainDescription:
            AuditSettings:
                EnableDmlAudit: true
                ExpectedSubjects:
                    0: "user2@ad"
                    1: "user1"
    ```

### Пример включения

```shell
cat >enable-dml-audit.txt <<END
ModifyScheme {
    OperationType: ESchemeOpAlterExtSubDomain
    WorkingDir: "/root"
    SubDomain {
        Name: "db"
        AuditSettings {
            EnableDmlAudit: true
            ExpectedSubjects: ["user2@ad", "user1"]
        }
    }
}
END
./ydbd --server=HOST database schema exec enable-dml-audit.txt
```

Изменение делается так же.

{% note info "" %}

`EnableDmlAudit` и `ExpectedSubjects` можно менять независимо друг от друга.

Новое значение `ExpectedSubjects` полностью заменяет старое. Очистка списка -- `ExpectedSubjects: [""]`.

{% endnote %}

## Что нужно знать

- Логирование происходит в компоненте {{ ydb-short-name }} `gRPC Proxy`.

- Логирование DML-операций не бесплатное, `EnableDmlAudit` и `ExpectedSubjects` на уровне базы помогают регулировать объём потока аудируемых событий.

- Логирование операций происходит без парсинга текста запроса, поэтому детальная информация по запросу (тип выражения или перечень затрагиваемых таблиц) недоступна.

- Как следствие, вместе с `UPDATE`, `INSERT`, `DELETE` и др. логируется и `SELECT`.

- Как следствие, текст запроса логируется со всеми литералами. Чтобы избежать попадания литеральных значений в лог, нужно использовать [запросы с параметрами](../reference/ydb-sdk/parameterized_queries.md).

- В лог попадают запросы, прошедшие [аутентификацию](../deploy/configuration/config#auth) и проверку прав на базу. Анонимные запросы не логируются (не проходят фильтр `ExpectedSubjects`).

- Для каждого запроса в аудитный лог выводится ровно одна запись, которая делается в момент завершения. `start_time` и `end_time` содержат время начала и завершения операции.

- В `query_text` многострочный текст запроса схлопывается в одну строку с удалением лишних пробельных символов, и затем обрезается по длине до 1024 байт.

- В `ExpectedSubjects` работают только User SID'ы пользователей, SID'ы групп указывать можно -- нет.

## Пример записи лога

```json
{
    "component": "grpc-proxy",
    "remote_address": "xxxx:xxx:xxxx:xx:xxxx:xxxx:xxxx:xxx",
    "subject": "bfbohb360qqqql1ef604@ad",
    "database": "/root/db",
    "operation": "ExecuteDataQueryRequest",
    "start_time": "2023-11-03T20:40:53.897285Z",
    "query_text": "--!syntax_v1 PRAGMA TablePathPrefix(\"/root/db/.sys_health\"); $values = ( SELECT t1.id AS id, t1.value AS t1_value, t2.value AS t2_value FROM table1 AS t1 INNER JOIN table2 AS t2 ON t1.id == t2.id ); UPSERT INTO table1 SELECT id, t1_value + 1 as value FROM $values; UPSERT INTO table2 SELECT id, t2_value + 1 as value FROM $values; SELECT COUNT(*) FROM $values;",
    "begin_tx": "1",
    "commit_tx": "1",
    "tx_id": "{none}",
    "end_time": "2023-11-03T20:40:53.950970Z",
    "status": "SUCCESS",
    "detailed_status": "SUCCESS"
}
```

[*popup-note]: Суффикс `Request` опущен для удобства чтения.<br>Например, для `ExecuteDataQuery` реальное значение в логе будет `ExecuteDataQueryRequest`.
