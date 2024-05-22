# DROP VIEW

`DROP VIEW` deletes an existing [view](../../../../concepts/datamodel/view).

## Syntax

```sql
DROP VIEW <name>
```

### Parameters

* `name` - the name of the view to be deleted.

## Examples

The following command will drop the view named `recent_series`:

```sql
DROP VIEW recent_series;
```

## See also

* [CREATE VIEW](create-view.md)
* [ALTER VIEW](alter-view.md)