## NTILE

Распределяет строки упорядоченного [раздела](../../../syntax/window.md#partition) в заданное количество групп. Группы нумеруются, начиная с единицы. Для каждой строки функция NTILE возвращает номер группы,которой принадлежит строка.

**Сигнатура**
```
NTILE(Uint64)->Uint64
```

**Примеры**
``` yql
SELECT
    NTILE(10) OVER w AS group_num
FROM my_table
WINDOW w AS (ORDER BY key);
```

