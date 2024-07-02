# Выборка данных из всех колонок

Выберите все колонки из таблицы с помощью оператора [SELECT](../../yql/reference/syntax/select/index.md).

{% include [yql-reference-prerequisites](_includes/yql_tutorial_prerequisites.md) %}

```sql
SELECT         -- Оператор выбора данных.

    *          -- Выбор всех колонок из таблицы.

FROM episodes; -- Таблица, из которой нужно выбрать данные.

COMMIT;
```
