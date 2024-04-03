# Добавление и удаление колонок

Добавьте новую колонку в таблицу, а затем удалите ее.

{% include [yql-reference-prerequisites](_includes/yql_tutorial_prerequisites.md) %}

## Добавить колонку {#add-column}
Добавьте неключевую колонку в существующую таблицу:

```sql
ALTER TABLE episodes ADD COLUMN viewers Uint64;
```
## Удалить колонку {#delete-column}
Удалите добавленную колонку из таблицы:

```sql
ALTER TABLE episodes DROP COLUMN viewers;
```
