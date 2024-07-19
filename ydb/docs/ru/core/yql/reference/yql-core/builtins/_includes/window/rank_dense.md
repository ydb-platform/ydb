## RANK / DENSE_RANK / PERCENT_RANK {#rank}

Пронумеровать группы соседних строк [раздела](../../../syntax/window.md#partition) с одинаковым значением выражения в аргументе. `DENSE_RANK` нумерует группы подряд, а `RANK` — пропускает `(N - 1)` значений, где `N` — число строк в предыдущей группе. `PERCENT_RANK` выдает относительный ранг текущей строки: `(RANK` - 1)/(число строк в разделе - 1)`.

При отсутствии аргумента использует порядок, указанный в секции `ORDER BY` определения окна.
Если аргумент отсутствует и `ORDER BY` не указан, то все строки считаются равными друг другу.

{% note info %}

Возможность передавать аргумент в `RANK`/`DENSE_RANK`/`PERCENT_RANK` является нестандартным расширением YQL.

{% endnote %}

**Сигнатура**
```
RANK([T])->Uint64
DENSE_RANK([T])->Uint64
PERCENT_RANK([T])->Double
```

**Примеры**
``` yql
SELECT
   RANK(my_column) OVER w
FROM my_table
WINDOW w AS (ORDER BY key);
```
``` yql
SELECT
   DENSE_RANK() OVER w
FROM my_table
WINDOW w AS (ORDER BY my_column);
```
``` yql
SELECT
   PERCENT_RANK() OVER w
FROM my_table
WINDOW w AS (ORDER BY my_column);
```

