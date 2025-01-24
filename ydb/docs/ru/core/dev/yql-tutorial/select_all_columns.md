# Выборка данных из всех колонок

Выберите все колонки из таблиц с помощью оператора [SELECT](../../yql/reference/syntax/select/index.md).

{% include [yql-reference-prerequisites](_includes/yql_tutorial_prerequisites.md) %}

```yql
SELECT         -- Оператор выбора данных.

    *          -- Выбор всех колонок из таблицы.

FROM `<table_name>`; -- Таблица, из которой нужно выбрать данные. 
                   -- Можно выбрать данные из таблиц: series, seasons, episodes.
```
