# ALTER VIEW

`ALTER VIEW` changes the definition of a [view](../../../../concepts/datamodel/view).

{% note warning %}

This feature is not supported yet.

{% endnote %}

Instead, you can redefine a view by dropping it and recreating it with a different query or options:
```sql
DROP VIEW redefined_view;
CREATE VIEW redefined_view ...;
```
Please note that the two statements are executed separately, unlike a single `ALTER VIEW` statement. If a view is recreated in this way, it might be possible to observe the view in a deleted state for a brief moment.

## See also

* [CREATE VIEW](create-view.md)
* [DROP VIEW](drop-view.md)