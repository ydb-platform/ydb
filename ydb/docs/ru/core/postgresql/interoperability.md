# Интероперабельность PostgreSQL и {{ydb-name}}

{% include [./_includes/alert.md](./_includes/alert_preview.md) %}

Поддержка исполнения запросов в {{ydb-name}} на синтаксисе PostgreSQL реализована с помощью слоя совместимости:
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
    - `database_name` - название базы данных в кластере. Может быть сложным именем, например, `mycluster/tenant1/database/`.
    - `user_name` - логин пользователя.


## Соответствие типов данных {#supported_types}

Система типов данных {{ydb-name}} и PostgreSQL похожи, но при этом не совпадают.

### Использование данных таблиц, созданных в YQL синтаксисе, в синтаксисе PostgreSQL {#topg}
Типы данных из {{ydb-name}} автоматически преобразовываются в соответствующие им типы PostgreSQL. Преобразование выполняется неявно с помощью команды [`ToPg`](./advanced.md#topg).

Пример:
1. Создадим таблицу {{ydb-name}} с помощью YQL-синтаксиса
    ```sql
    CREATE TABLE `test_table`(col INT, PRIMARY KEY(col));
    ```
1. Добавим туда тестовые данные
    ```sql
    INSERT INTO test_table(col) VALUES(1)
    ```
1. Прочитаем эти данные с помощью PostgreSQL-синтаксиса. YQL-тип данных `INT` был автоматически переведен в тип `pgint4`, над которым была выполнена операция инкремента.
    ```
    psql -c "SELECT col+1 AS col FROM test_table"
    col
    ---
    2
    (1 row)
    ```

 Таблица соответствия типов данных YQL, при их использовании в PostgreSQL запросах:

{% include [topg](_includes/topg.md) %}


### Использование данных таблиц, созданных в PostgreSQL синтаксисе, в синтаксисе YQL {#frompg}

Для использования данных таблиц, созданных в PostgreSQL синтаксисе, необходимо явное преобразование типов с помощью функции [`FromPg`](./advanced.md#frompg).

Пример:
1. Создадим таблицу {{ydb-name}} с помощью PostgreSQL-синтаксиса
    ```sql
    CREATE TABLE test_table_pg(col INT, PRIMARY KEY(col));
    ```
1. Добавим туда тестовые данные
    ```sql
    INSERT INTO test_table_pg(col) VALUES(10)
    ```
1. Прочитаем эти данные с помощью YQL-синтаксиса. Для чтения данных, созданных с помощью PostgreSQL синтаксиса, необходимо явное преобразование c помощью функции [`FromPg`](./advanced.md#frompgtopg).
    ```
    ydb sql -s "SELECT FromPg(col)+1 AS col FROM test_table_pg"
    col
    ---
    11
    (1 row)
    ```

Таблица соответствия типов данных PostgreSQL, при их использовании в запросах на YQL-синтаксисе:

{% include [frompg](_includes/frompg.md) %}

Поддерживаются только преобразования типов приведенное выше.

Если необходимо выполнить преобразование типов данных, не указанных в таблице выше, то можно использовать промежуточное преобразование через тип `text`.

Например, выполним преобразование типа данных PostgreSQL - `money`:
```sql
SELECT FromPg(PgCast(PgConst("{1234}", _PgMoney), pgtext))
```
