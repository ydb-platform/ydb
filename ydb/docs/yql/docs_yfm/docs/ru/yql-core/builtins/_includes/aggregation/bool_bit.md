## BOOL_AND, BOOL_OR и BOOL_XOR {#bool-and-or-xor}

Применение соответствующей логической операции (`AND`/`OR`/`XOR`) ко всем значениям булевой колонки или выражения.

Эти функции **не пропускают** `NULL` значение при агрегации, единственное `NULL` значение превратит результат в `NULL`. Для агрегации с пропуском `NULL`-ов можно использовать функции `MIN`/`MAX` или `BIT_AND`/`BIT_OR`/`BIT_XOR`.

**Примеры**
``` yql
SELECT
  BOOL_AND(bool_column),
  BOOL_OR(bool_column),
  BOOL_XOR(bool_column)
FROM my_table;
```

## BIT_AND, BIT_OR и BIT_XOR {#bit-and-or-xor}

Применение соответствующей битовой операции ко всем значениям числовой колонки или выражения.

**Примеры**
``` yql
SELECT
    BIT_XOR(unsigned_numeric_value)
FROM my_table;
```
