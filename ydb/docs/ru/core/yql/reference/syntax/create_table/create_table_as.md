# CREATE TABLE AS

## Синтаксис CREATE TABLE AS

Вызов `CREATE TABLE AS` создает новую {% if concept_table %}[таблицу]({{ concept_table }}){% else %}таблицу{% endif %}, которая заполнена данными из результатов запроса.

```
CREATE TABLE table_name (
    PRIMARY KEY ( column, ... )
)
WITH ( key = value, ... )
AS SELECT ...
```

Имена и типы столбцов будут соответствовать результатам `SELECT`.
Для колонок не [опционального типа](../../types/optional.md) также будет выставлен модификатор `NOT NULL`.

При создании таблицы через `CREATE TABLE AS` не поддерживается указание колонок, [вторичных индексов](secondary_index.md), [групп колонок](family.md). При этом поддерживаются [дополнительные параметры](with.md).


## Особенности

{% note warning %}

Отсутствуют гарантии на порядок записи строк в таблицу при использовании `CREATE TABLE AS`.
Если `SELECT` вернул 2 строки с одинаковым ключом, то после завершения выполнения `CREATE TABLE AS` с данным ключом в таблице может быть любая из них.
 
{% endnote %}


* `CREATE TABLE AS` поддерживается только в режиме [неявного контроля транзакций](../../../concepts/transactions.md#implicit).

* `CREATE TABLE AS` может быть только единственным выражением в запросе.

* `CREATE TABLE AS` не имеет ограничений на записываемый объем данных.

* `CREATE TABLE AS` не использует локи и устойчив к балансировке и разделению [таблеток](./concepts/glossary#tablet). Все чтения производятся из консистентного снапшота. С другими транзакциями `CREATE TABLE AS` не конфликтует.

* `CREATE TABLE AS` позволяет использовать в одном запросе и [столбцовые таблицы](../glossary.md#column-oriented-table), и [строковые таблицы](../glossary.md#row-oriented-table).

* `CREATE TABLE AS` работает через создание [временной таблицы](temporary.md) и перемещение ее по нужному пути после окончания записи. В связи с этим при падениях запроса в некоторых случаях (например, при отказе ноды с сессией, выполнявшей `CREATE TABLE AS`) временная таблица может быть удалена не сразу и в течение небольшого промежутка времени занимать место на диске.

## Примеры

{% list tabs %}

- Создание строковой таблицы с одной строкой

```
CREATE TABLE my_table (
    PRIMARY KEY (key)
) AS SELECT 
    1 AS key
    "test" AS value;
```

- Создание столбцовой таблицы из результатов запроса

```
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