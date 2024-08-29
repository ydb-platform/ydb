# DELETE FROM (удаление строк из таблицы)

{% include [../_includes/alert_preview.md)](../_includes/alert_preview.md) %}

Синтаксис инструкции `DELETE FROM`:
 ```sql
 DELETE FROM <table name>
 WHERE <column name><condition><value/range>;
 ```
Для удаления строки из таблицы по конкретному значению столбца используется конструкция `DELETE FROM <table name> WHERE <column name><condition><value/range>`.


{% note warning %}

Обратите внимание, что использование оператора `WHERE ...` опционально, поэтому при работе с `DELETE FROM` очень важно случайно не выполнить команду раньше указания оператора `WHERE ...`.

{% endnote %}


{% include [../_includes/alert_locks.md](../_includes/alert_locks.md) %}