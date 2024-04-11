# ALTER VIEW

`ALTER VIEW` изменяет определение представления.

Это команда не поддерживается в текущей версии {{ ydb-short-name }}.
Для переопределения представления можно его удалить и воссоздать с другим определением:
```sql
DROP VIEW redefined_view;
CREATE VIEW redefined_view ...;
```
Но этот запрос не будет атомарным.

## См. также

* [CREATE VIEW](create-view.md)
* [DROP VIEW](drop-view.md)