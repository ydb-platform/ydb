## TableRow{% if feature_join %}, JoinTableRow{% endif %} {#tablerow}

Получение всей строки таблицы целиком в виде структуры. Аргументов нет{% if feature_join %}. `JoinTableRow` в случае `JOIN`-ов всегда возвращает структуру с префиксами таблиц{% endif %}.

**Сигнатура**
```
TableRow()->Struct
```

**Пример**
``` yql
SELECT TableRow() FROM my_table;
```
