# Интероперабельность PostgreSQL и {{ydb-name}}

{% include [./_includes/alert.md](./_includes/alert_preview.md) %}

Поддержка исполнения запросов в {{ydb-name}} в синтаксисе PostgreSQL реализована с помощью слоя совместимости:
1. Программа отправляет запросы в {{ ydb-short-name }}, где их обрабатывает компонент под названием `pgwire`. Pgwire реализует [сетевой протокол](https://www.postgresql.org/docs/16/protocol.html) PostgreSQL и передает команды в query processor.
2. Query processor транслирует PostgreSQL запросы в YQL AST.
3. После обработки запросов результаты собираются и отправляются обратно в программу, отправившую запрос, по сетевому протоколу PostgreSQL. При обработке запроса он может распараллеливаться и исполняться на произвольном количестве узлов {{ydb-name}}.

Графически работу PostgreSQL совместимости можно представить так:
![Схема работы PostgreSQL совместимости](./_includes/ydb_pg_scheme.png)

Такая архитектура интеграции с PostgreSQL позволяет выполнять запросы на PostgreSQL над {{ydb-name}} типами данных и наоборот, выполнять YQL-запросы над типами данных PostgreSQL, обеспечивая интероперабельность работы с данными.

Проиллюстрируем это с помощью следующего сценария:
1. Создадим таблицу {{ydb-name}} с помощью YQL-синтаксиса
    ```sql
    CREATE TABLE `test_table`(col INT, PRIMARY KEY(col));
    ```
1. Добавим туда тестовые данные
    ```sql
    INSERT INTO test_table(col) VALUES(1)
    ```
1. Прочитаем эти данные с помощью PostgreSQL-синтаксиса
    ```
    psql -h <ydb_address> -d <database_name> -U <user_name> -c "SELECT * FROM test_table"
    col
    ---
    1
    (1 row)
    ```

    Где:
    - `ydb_address` - адрес кластера ydb, к которому выполняется подключение.
    - `database_name` - название базы данных в кластере. Может быть сложным именем, например, `mycluster/tenant1/database`.
    - `user_name` - логин пользователя.


## Соответствие типов данных {#supported_types}

Система типов данных {{ydb-name}} и PostgreSQL похожи, но при этом не совпадают.

### Использование данных таблиц, созданных в YQL синтаксисе, в синтаксисе PostgreSQL {#topg}
Типы данных из {{ydb-name}} автоматически преобразовываются в соответствующие им типы PostgreSQL. Преобразование выполняется неявно с помощью команды [`ToPg`](../yql/reference/udf/list/postgres.md#topg).

Пример:
1. Создадим таблицу {{ydb-name}} с помощью YQL-синтаксиса
    ```sql
    CREATE TABLE `test_table`(col INT, PRIMARY KEY(col));
    ```
1. Добавим туда тестовые данные
    ```sql
    INSERT INTO test_table(col) VALUES(1)
    ```
1. Прочитаем эти данные с помощью PostgreSQL-синтаксиса. YQL-тип данных `INT` был автоматически переведен в PostgreSQL-тип `int4`, над которым была выполнена операция инкремента.
    ```
    psql -c "SELECT col+1 AS col FROM test_table"
    col
    ---
    2
    (1 row)
    ```

Так как все вычисления выполняются внутри YDB, то для для каждого PostgreSQL типа создан "зеркальный тип" в YDB. Например, тип `text` из PostgreSQL при обработке внутри YDB будет иметь тип `pgtext`. Это сделано, чтобы обеспечить точную семантику работы типов PostgreSQL внутри YDB. При преобразовании типа из PostgreSQL в YDB применяется правило, что для каждого такого типа добавляется префикс `pg`, после чего используется оригинальное имя типа из PostgreSQL.

Таблица соответствия типов данных YQL, при их использовании в PostgreSQL запросах:

{% include [topg](../yql/reference/udf/list/_includes/topg.md) %}


### Использование данных таблиц, созданных в PostgreSQL синтаксисе, в синтаксисе YQL {#frompg}

При использовании данных из таблиц, созданных в PostgreSQL синтаксисе, YQL интерпретирует данные в этих колонках, как специальные типы из семейства `pg*`.

Пример:
1. Создадим таблицу {{ydb-name}} с помощью PostgreSQL-синтаксиса
    ```sql
    CREATE TABLE test_table_pg(numeric INT, PRIMARY KEY(col));
    ```
1. Добавим туда тестовые данные
    ```sql
    INSERT INTO test_table_pg(col) VALUES(10)
    ```
1. Прочитаем эти данные с помощью YQL-синтаксиса
    ```
    ydb sql -s "SELECT col+1 AS col FROM test_table_pg"
    col
    ---
    "11" -- pgnumeric
    (1 row)
    ```

Правила преобразования типов PostgreSQL в типы YQL приведены в таблице:

|PostgreSQL | YQL|
|---|---|
| `bool` |`pgbool` |
| `int2` |`pgint2` |
| `int4` |`pgint4` |
| `int8` |`pgint8` |
|`numeric` |`pgnumeric` |
| `float4` |`pgfloat4` |
| `float8` |`pgfloat8` |
| `bytea` |`pgbytea` |
| `text` |`pgtext` |
| `bytea` |`pgbytea` |
| `json` |`pgjson` |
| `uuid` |`pguuid` |
| `jsonb` |`pgjsonb` |
| `date` |`pgdate` |
| `timestamp` |`pgtimestamp` |
| `interval` | `pginterval` |
| `text` |`pgtext` |
| `date` | `pgdate`|
| `timestamp` |`pgtimestamp` |
| `interval` |`pginterval` |
| `numeric` |`pgnumeric` |

Встроенные функции YQL ориентированы на работу с собственными типами данных, например, `Ip::FromString` получает на вход типы данных `Utf8` или `String`. Поэтому встроенные функции YQL не могут работать с типами данных PostgreSQL. Для решения задачи конвертации типов существует функция [`FromPg`](../yql/reference/udf/list/postgres.md#frompg), выполняющая преобразование данных из типов PostgreSQL в типы YQL.

Пример:
1. Создадим таблицу {{ydb-name}} с помощью PostgreSQL-синтаксиса
    ```sql
    CREATE TABLE test_table_pg_ip(col text, PRIMARY KEY(col));
    ```
1. Добавим туда тестовые данные
    ```sql
    INSERT INTO test_table_pg_ip(col) VALUES('::ffff:77.75.155.3')
    ```
1. Прочитаем эти данные с помощью YQL-синтаксиса:
    ```
    ydb sql -s "SELECT Ip::ToString(Ip::GetSubnet(Ip::FromString(col))) AS subnet
        FROM test_table_pg_ip"
    Status: GENERIC_ERROR
    Issues:
    <main>: Error: Type annotation, code: 1030
        <main>:1:1: Error: At function: RemovePrefixMembers, At function: PersistableRepr, At function: SqlProject, At function: SqlProjectItem
            <main>:1:12: Error: At function: Apply, Callable is produced by Udf: Ip.FromString
                <main>:1:23: Error: Mismatch type argument #1, type diff: String!=pgtext
    ```
1. Для работы функции `Ip::FromString` необходимо предварительно выполнить преобразование типов данных с помощью функции `PgFrom`:
    ```
    ydb sql -s "SELECT Ip::ToString(Ip::GetSubnet(Ip::FromString(FromPg(col))) AS subnet
        FROM test_table_pg_ip"
    ┌────────┐
    │ subnet │
    ├────────┤
    │ "::"   │
    └────────┘
    ```

Кроме этого, при работе с PostgreSQL типами данных [можно использовать функции самого PostgreSQL](../yql/reference/udf/list/postgres.md#callpgfunction) непосредственно из YQL-синтаксиса:

1. Создадим таблицу {{ydb-name}} с помощью PostgreSQL-синтаксиса
    ```sql
    CREATE TABLE test_table_array(col INT, col2 _INT, PRIMARY KEY(col));
    ```
1. Добавим туда тестовые данные
    ```sql
    INSERT INTO test_table_array(col, col2) VALUES(1, '{1,2,3}')
    ```
1. Вызовем встроенную функцию PostgreSQL `array_length`:
    ```
    ydb sql -s "select Pg::array_length(col2, 1) FROM test_table_array"
    col
    ---
    "3" -- число элементов в массиве
    (1 row)
    ```
