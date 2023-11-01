# DROP TABLE (удаление таблицы)

{% include [../_includes/alert_preview.md)](../_includes/alert_preview.md) %}

Синтаксис инструкции `DROP TABLE`:
```sql
DROP TABLE [IF EXISTS] <table name>;
```
Инструкция `DROP TABLE <table name>;` предназначена для удаления таблицы. Например: `DROP TABLE people;`. Если удаляемая таблица отсутствует – будет выведено сообщение об ошибки:
```
Error: Cannot find table '...' because it does not exist or you do not have access permissions. Please check correctness of table path and user permissions., code: 2003
```

В ряде сценариев такое поведение не требуется. Например, если мы хотим гарантированно создать новую таблицу с удалением предыдущий в рамках одного SQL-скрипта или последовательности SQL-команд. В таких случаях применяется инструкция `DROP TABLE IF EXIST <table name>`. В случае отсутствия таблицы инструкция вернет сообщение `DROP TABLE`, а не ошибку.

{% include [../_includes/alert_locks.md](../_includes/alert_locks.md) %}