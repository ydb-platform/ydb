# Заполнение таблицы данными из запроса

Вызов `CREATE TABLE AS` создает новую {% if concept_table %}[таблицу]({{ concept_table }}){% else %}таблицу{% endif %}, которая заполнена данными из результатов запроса.

```yql
CREATE TABLE table_name (
    PRIMARY KEY ( column, ... )
)
WITH ( key = value, ... )
AS SELECT ...
```

Имена и типы колонок будут соответствовать результатам `SELECT`.
Для колонок [неопционального типа](../../types/optional.md) также будет выставлен модификатор `NOT NULL`.

При создании таблицы через `CREATE TABLE AS` не поддерживается указание имен колонок (имена колонок созданной таблицы берутся из результата запроса), [вторичных индексов](secondary_index.md), [векторных индексов](vector_index.md), [групп колонок](family.md). Все вышеперечисленное можно изменять при помощи [`ALTER TABLE`](../alter_table/index.md) после создания таблицы. При этом поддерживаются [дополнительные параметры](with.md).


## Особенности

{% note warning %}

Запись строк производится с полной перезаписью строки, как при использовании [`REPLACE INTO`](../replace_into.md), но при этом отсутствуют гарантии на порядок записи строк в таблицу.

Если `SELECT` вернул 2 или более строки с одним и тем же значением первичного ключа, то после завершения выполнения `CREATE TABLE AS` в созданной таблице будет только одна запись с таким значением первичного ключа. При этом какая именно из записей найденных в `SELECT` будет добавлена - в общем случае не определено.

{% endnote %}


* `CREATE TABLE AS` поддерживается только в режиме [неявного контроля транзакций](../../../../concepts/transactions.md#implicit). Таблица появится по указанному пути уже заполненной.

* `CREATE TABLE AS` может быть только единственным [DML](https://en.wikipedia.org/wiki/Data_manipulation_language)/[DDL](https://en.wikipedia.org/wiki/Data_definition_language) выражением в запросе. Допустимо использование [PRAGMA](../pragma.md), [DECLARE](../declare.md) и [именованных выражений](../expressions.md#named-nodes).

* `CREATE TABLE AS` не конфликтует с другими транзакциями. При выполнении запроса не используются блокировки, а все чтения производятся из консистентного снапшота. Балансировка или разделение [таблеток](../../../../concepts/glossary.md#tablet) не приводят к ошибкам.

* `CREATE TABLE AS` позволяет использовать в одном запросе и [колоночные таблицы](../../../../concepts/glossary.md#column-oriented-table), и [строковые таблицы](../../../../concepts/glossary.md#row-oriented-table).

* `CREATE TABLE AS` при выполнении создаёт [временную таблицу](temporary.md) и перемещает её по нужному пути после окончания записи данных. Если при выполнении `CREATE TABLE AS` произошла ошибка приведшая к прерыванию выполнения команды, то временная таблица может быть удалена не сразу, а в течение небольшого промежутка времени.

## Примеры

{% list tabs %}

- Создание строковой таблицы с одной строкой

    ```yql
    CREATE TABLE my_table (
        PRIMARY KEY (key)
    ) AS SELECT 
        1 AS key,
        "test" AS value;
    ```

- Создание колоночной таблицы из результатов запроса

    ```yql
    CREATE TABLE my_table (
        PRIMARY KEY (key1, key2)
    ) WITH (
        STORE=COLUMN
    ) AS SELECT 
        key AS key1,
        Unwrap(other_key) AS key2,
        value,
        String::Contains(value, "test") AS has_test
    FROM other_table;
    ```

{% endlist %}
