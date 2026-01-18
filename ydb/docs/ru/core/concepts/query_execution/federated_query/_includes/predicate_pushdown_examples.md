|Описание|Пример|
|---|---|
|Проверка на `NULL`|`WHERE column1 IS NULL` или `WHERE column1 IS NOT NULL`|
|Логических условий `OR`, `NOT`, `AND` и круглых скобок для управление приоритетом вычислений. |`WHERE column1 IS NULL OR (column2 IS NOT NULL AND column3 > 10)`.|
|[Операторов сравнения](../../../../yql/reference/syntax/expressions.md#comparison-operators) c другими колонками или константами. |`WHERE column1 > column2 OR column3 <= 10`, `WHERE column1 + column2 > 10`, `WHERE column1 = (10 + 10)`|

При использовании других видов фильтров пушдаун на источник не выполняется: фильтрация строк внешней таблицы будет выполнена на стороне федеративной {{ ydb-short-name }}, что означает, что {{ ydb-short-name }} выполнит полное чтение (full scan) внешней таблицы в момент обработки запроса.