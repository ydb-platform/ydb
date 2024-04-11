# ALTER VIEW

`ALTER VIEW` changes the definition of a view.

This feature is not supported yet. You can redefine a view by dropping it and recreating it with a different query or options values:
```sql
DROP VIEW redefined_view;
CREATE VIEW redefined_view ...;
```
This script would not be atomic though.

## See also

* [CREATE VIEW](create-view.md)
* [DROP VIEW](drop-view.md)