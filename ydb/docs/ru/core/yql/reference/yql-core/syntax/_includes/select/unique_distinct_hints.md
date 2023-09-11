## UNIQUE DISTINCT hints {#unique_distinct_hints}

Непосредственно после `SELECT` возмоно добавить [SQL хинты](../lexer.md#sql-hints) `unique` или `distinct` которые заявляют, что эта проекция порождает данные, содержащие уникальные значения в указаном наборе колонок. Это может, использоваться для оптимизации следующих подзапросов, выполняющихся над этой проекцией, или для записи в мета-атрибуты таблицы при `INSERT`.

* Колонки указываются в значениях хинта через пробел.
* Если набор колонок не задан, значит уникальность распостаняется на полный набор колонок этой проекции.
* `unique` - означает уникальные либо `null` значения. По станларту SQL каждый null уникален: NULL = NULL -> NULL
* `distinct` - означает полностью уникальные значение включая `null`: NULL IS DISTINCT FROM NULL -> FALSE
* Можно указать несколько наборов колонок в нескольких хинтах у одной проекции.
* Если хинт содержит колонку, которой нет в проекции, он будет проигнорирован.

**Примеры**

``` yql
SELECT /*+ unique() */ * FROM Input;
SELECT /*+ distinct() */ * FROM Input;

SELECT /*+ distinct(key subkey) */ * FROM Input;
SELECT /*+ unique(key) distinct(subkey value) */ * FROM Input;

-- Missed column - ignore hint.
SELECT /*+ unique(subkey value) */ key, value FROM Input;
```
