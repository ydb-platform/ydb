# Добавление и удаление колонок

Добавьте новую колонку в строковую или колоночную таблицу, а затем удалите ее.

{% include [yql-reference-prerequisites](_includes/yql_tutorial_prerequisites.md) %}

## Добавить колонку {#add-column}

Добавьте неключевую колонку в существующую строковую или колоночную таблицу:

```yql
ALTER TABLE episodes ADD COLUMN viewers Uint64;
```

## Удалить колонку {#delete-column}

Удалите добавленную колонку из строковой или колоночной таблицы:

```yql
ALTER TABLE episodes DROP COLUMN viewers;
```
