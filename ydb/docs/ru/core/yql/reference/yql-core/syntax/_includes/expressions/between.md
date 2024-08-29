## BETWEEN {#between}

Проверка на вхождение значения в диапазон. Cинтаксис: `expr [NOT] BETWEEN [ASYMMETRIC | SYMMETRIC] expr AND expr`.
* `BETWEEN` и `BETWEEN ASYMMETRIC` эквивалентны, `x BETWEEN a AND b` эквивалентно `a <= x AND x <= b`.
* `BETWEEN SYMMETRIC` автоматически переставляет аргументы местами так чтобы диапазон получился непустым,
`x BETWEEN SYMMETRIC a AND b` эквивалентно `(x BETWEEN a AND b) OR (x BETWEEN b AND a)`.
* `NOT` инвертирует результат проверки.

**Примеры**

``` yql
SELECT * FROM my_table
WHERE key BETWEEN 10 AND 20;
```

``` yql
SELECT * FROM my_table
WHERE key NOT BETWEEN SYMMETRIC 20 AND 10;
```
