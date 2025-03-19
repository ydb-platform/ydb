
# DISCARD

Вычисляет [`SELECT`](select/index.md), [`REDUCE`](reduce.md) или [`PROCESS`](process.md), но не возвращает результат ни в клиент, ни в таблицу. Не может быть задано одновременно с [INTO RESULT](into_result.md).

Полезно использовать в сочетании с [`Ensure`](../builtins/basic.md#ensure) для проверки выполнения пользовательских условий на финальный результат вычислений.

### Примеры

```yql
DISCARD SELECT 1;
```

```yql
INSERT INTO result_table WITH TRUNCATE
SELECT * FROM
my_table
WHERE value % 2 == 0;

COMMIT;

DISCARD SELECT Ensure(
    0, -- will discard result anyway
    COUNT(*) > 1000,
    "Too small result table, got only " || CAST(COUNT(*) AS String) || " rows"
) FROM result_table;

```
