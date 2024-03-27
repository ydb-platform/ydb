# ALTER VIEW

ALTER VIEW — изменить определение представления.

Это команда не поддерживается в текущей версии {{ ydb-short-name }}.
Для переопределения представления можно его удалить и воссоздать с другим определением:
```sql
DROP VIEW redefined_view;
CREATE VIEW redefined_view ...;
```

## См. также

[CREATE VIEW](create_view.md), [DROP VIEW](drop_view.md)