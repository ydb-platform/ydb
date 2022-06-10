## IF {#if}

Проверяет условие `IF(condition_expression, then_expression, else_expression)`.

Является упрощенной альтернативой для [CASE WHEN ... THEN ... ELSE ... END](../../../syntax/expressions.md#case).

**Сигнатура**
```
IF(Bool, T, T)->T
IF(Bool, T)->T?
```

Аргумент `else_expression` можно не указывать. В этом случае, если условие ложно (`condition_expression` вернул `false`), будет возвращено пустое значение с типом, соответствующим `then_expression` и допускающим значение `NULL`. Таким образом, у результата получится [optional тип данных](../../../types/optional.md).

**Примеры**
``` yql
SELECT
  IF(foo > 0, bar, baz) AS bar_or_baz,
  IF(foo > 0, foo) AS only_positive_foo
FROM my_table;
```
