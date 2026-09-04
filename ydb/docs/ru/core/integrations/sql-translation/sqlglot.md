# SQLGlot

[SQLGlot](https://github.com/tobymao/sqlglot) — это написанный на чистом Python парсер, транспилер, оптимизатор и форматировщик SQL, поддерживающий более двадцати диалектов (PostgreSQL, MySQL, ClickHouse, BigQuery, Snowflake, Spark SQL и других). SQLGlot разбирает запрос в абстрактное синтаксическое дерево (AST), которое можно анализировать и преобразовывать программно, а затем сгенерировать обратно в SQL на любом из поддерживаемых диалектов.

[Плагин ydb-sqlglot-plugin](https://github.com/ydb-platform/ydb-sqlglot-plugin) добавляет в SQLGlot диалект {{ ydb-short-name }}. После его установки SQLGlot умеет как разбирать запросы на [YQL](../../yql/reference/index.md), так и генерировать YQL из запросов, написанных на других диалектах. Преобразование двунаправленное: любой поддерживаемый диалект ↔ YQL.

В отличие от готового [конвертера SQL-диалектов](sql-dialect-converter.md), который работает как внешний сервис и встроен в графические инструменты, плагин — это библиотека, которую вы подключаете в собственный Python-код. Это даёт два важных преимущества:

- **Локальная обработка.** Запросы не покидают вашу машину — никакие данные не отправляются на внешний HTTPS-сервис. Подходит для работы с конфиденциальными запросами.
- **Программный доступ к AST.** Кроме транспиляции, доступны разбор запроса, анализ происхождения столбцов (column lineage), оптимизация и форматирование — всё, что умеет SQLGlot.

## Возможности {#features}

- Транспиляция запросов из других диалектов в YQL и обратно.
- Разбор YQL-запроса в AST для программного анализа и модификации.
- Анализ происхождения столбцов (column lineage) — отслеживание того, из каких таблиц и колонок получается каждый результирующий столбец.
- Оптимизация и форматирование запросов средствами SQLGlot.
- Поддержка специфичных для YQL конструкций: именованных выражений (`$variable`), модульных функций (`DateTime::GetYear()`), `FLATTEN`, лямбда-выражений, контейнерных типов и других.

{% note info %}

Полный список поддерживаемого функционала и актуальные ограничения приведены в [README репозитория плагина](https://github.com/ydb-platform/ydb-sqlglot-plugin).

{% endnote %}

## Установка {#install}

Плагин публикуется в PyPI под именем `ydb-sqlglot-plugin`:

```bash
pip install ydb-sqlglot-plugin
```

Требования:

- Python 3.9 или новее.
- SQLGlot версии 28.6.0 или новее (устанавливается автоматически как зависимость).

Плагин распространяется под лицензией Apache 2.0.

## Быстрый старт {#quickstart}

После установки диалект {{ ydb-short-name }} доступен в SQLGlot автоматически — дополнительные импорты не нужны. У него два эквивалентных имени, `ydb` и `yql`; любое из них можно указывать в аргументах `read` и `write`.

Преобразование запроса из MySQL в YQL:

```python
import sqlglot

result = sqlglot.transpile(
    "SELECT * FROM users WHERE id = 1",
    read="mysql",
    write="ydb",
)[0]

print(result)
# SELECT * FROM `users` WHERE id = 1
```

Обратное преобразование — из YQL в PostgreSQL. Именованное выражение YQL (`$t = (...)`) превращается в CTE (`WITH ... AS`):

```python
import sqlglot

result = sqlglot.transpile(
    "$t = (SELECT id FROM users); SELECT * FROM $t AS t",
    read="ydb",
    write="postgres",
)[0]

print(result)
# WITH t AS (SELECT id FROM users) SELECT * FROM t AS t
```

{% note tip %}

Функция `sqlglot.transpile()` возвращает список строк — по одной на каждый оператор в исходном тексте. Если в запросе один оператор, берите первый элемент (`[0]`).

{% endnote %}

## Примеры использования {#usage}

### Перенос запросов из других СУБД {#migration}

При миграции в {{ ydb-short-name }} плагин автоматически приводит конструкции исходного диалекта к правилам YQL. Например, имена с указанием схемы (`schema.table`) преобразуются в [путь {{ ydb-short-name }}](../../concepts/datamodel/dir.md) с обратными кавычками:

```python
import sqlglot

print(sqlglot.transpile("SELECT * FROM analytics.events", read="postgres", write="ydb")[0])
# SELECT * FROM `analytics/events`
```

Коррелированные подзапросы, не поддерживаемые в {{ ydb-short-name }} напрямую, по возможности переписываются в `JOIN`, а конструкции `WITH` — в именованные выражения YQL.

### Разбор запроса в AST {#parse}

Если нужна не только транспиляция, но и анализ структуры запроса, разберите его в дерево с помощью `sqlglot.parse_one()`:

```python
import sqlglot
from sqlglot import exp

tree = sqlglot.parse_one("SELECT id, name FROM `users` WHERE age > 18", dialect="ydb")

# Перечислить все таблицы, к которым обращается запрос
for table in tree.find_all(exp.Table):
    print(table.name)
# users

# Сгенерировать запрос обратно — на любом диалекте
print(tree.sql(dialect="clickhouse"))
```

### Анализ происхождения столбцов {#lineage}

Поскольку YQL разбирается в стандартное AST SQLGlot, для запросов на YQL работает встроенный в SQLGlot анализ происхождения данных (column lineage). Это позволяет проследить, из каких исходных таблиц и колонок получается каждый результирующий столбец:

```python
from sqlglot.lineage import lineage

node = lineage(
    "total",
    "SELECT SUM(amount) AS total FROM orders",
    dialect="ydb",
)

print(node.name)
# total
```

Анализ происхождения полезен при построении инструментов документирования данных, оценке влияния изменений схемы (impact analysis) и аудите запросов.

## Сопоставление функций и типов {#mappings}

Плагин содержит таблицы соответствий между стандартными SQL-конструкциями и их аналогами в {{ ydb-short-name }}.

Типы данных приводятся к эквивалентам {{ ydb-short-name }}:

| Тип в исходном диалекте | {{ ydb-short-name }}     |
|-------------------|--------------------------|
| `TINYINT`         | `Int8`                   |
| `INT`             | `Int32`                  |
| `BIGINT`          | `Int64`                  |
| `VARCHAR`, `TEXT` | `Utf8`                   |
| `TIMESTAMP`       | `Timestamp`              |

Функции сопоставляются по семейству значений:

- **Дата и время:** `DATE_TRUNC`, `EXTRACT`, операции с интервалами.
- **Строки:** `CONCAT`, `UPPER`, `LOWER`, `LENGTH` и другие.
- **Коллекции:** `ARRAY`, `ARRAY_FILTER`, `UNNEST` → `FLATTEN BY`.
- **Условные и числовые:** `NULLIF`, `ROUND`, `COUNT()`.
- **JSON:** `JSON_VALUE`, `JSON_QUERY` с поддержкой сопутствующих конструкций.

## См. также {#see-also}

- [{#T}](sql-dialect-converter.md)
- [ydb-sqlglot-plugin на GitHub](https://github.com/ydb-platform/ydb-sqlglot-plugin)
- [SQLGlot](https://github.com/tobymao/sqlglot)
- [YQL Reference](../../yql/reference/index.md)
