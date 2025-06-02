# SHOW CREATE

`SHOW CREATE` возвращает запрос, возможно состоящий из нескольких SQL-стейтментов, необходимых для воссоздания структуры выбранного объекта: {% if concept_table %}[таблицы]({{ concept_table }}){% else %}таблицы{% endif %} (`TABLE`) или [представления](../../../concepts/datamodel/view.md) (`VIEW`).

### Синтаксис:

```yql
SHOW CREATE [TABLE|VIEW] <name>;
```

### Параметры

* `TABLE|VIEW` - тип объекта: `TABLE` для таблицы или `VIEW` для представления.
* `<name>` - имя объекта, также может быть указан абсолютный путь.

## Результат выполнения

Команда возвращает **ровно одну строку** с тремя колонками:

| TablePath       | TableType | CreateTableQuery            |
|-----------------|-----------|-----------------------------|
| Абсолютный путь | Table/View| SQL-стейтменты для создания |

- **TablePath** — абсолютный путь к объекту (например, `/Root/MyTable` или `/Root/MyView`).
- **TableType** — тип объекта: `Table` или `View`.
- **CreateTableQuery** — полный набор DDL-стейтментов, необходимых для воссоздания объекта:
    - Для таблиц: основной оператор [CREATE TABLE](create_table/index.md) (с путем относительно базы), а также дополнительные команды, необходимые для описания текущего состояния и настроек:
        - [ALTER TABLE ... ALTER INDEX](alter_table/indexes#изменение-параметров-индекса-alter-index)— для задания настроек партицирования вторичных индексов.
        - [ALTER TABLE ... ADD CHANGEFEED](alter_table/changefeed.md)— для добавления потока изменений.
        - `ALTER SEQUENCE` — для восстановления состояния `Sequence` у колонок типа [Serial](../../../yql/reference/types/serial.md).
    - Для представлений: определение посредством команды [CREATE VIEW](create-view.md), а также, если необходимо, вывода [PRAGMA TablePathPrefix](pragma#tablepathprefix-table-path-prefix).

