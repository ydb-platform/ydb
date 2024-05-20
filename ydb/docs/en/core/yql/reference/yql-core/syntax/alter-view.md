# ALTER VIEW

`ALTER VIEW` changes the definition of a view.

{% note warning %}

This feature is not supported yet.

{% endnote %}

Instead, you can redefine a view by dropping it and recreating it with a different query or options:
```sql
DROP VIEW redefined_view;
CREATE VIEW redefined_view ...;
```
Such a query would not be atomic, though.

## See also

* [CREATE VIEW](create-view.md)
* [DROP VIEW](drop-view.md)