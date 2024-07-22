# Добавление и удаление колонок

Добавьте новую колонку в строковую или колоночную таблицу, а затем удалите ее.

{% include [yql-reference-prerequisites](_includes/yql_tutorial_prerequisites.md) %}

## Добавить колонку {#add-column}
Добавьте неключевую колонку в существующую строковую или колоночную таблицу:

```sql
ALTER TABLE episodes ADD COLUMN viewers Uint64;
```
## Удалить колонку {#delete-column}
Удалите добавленную колонку из строковой или колоночной таблицы:

```sql
ALTER TABLE episodes DROP COLUMN viewers;
```
