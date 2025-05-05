# DROP VIEW

`DROP VIEW` deletes an existing [view](concepts/datamodel/view).

## Syntax

```yql
DROP VIEW [IF EXISTS] <name>
```

### Parameters

* `IF EXISTS` - when specified, the statement does not return an error if a view with the given name does not exist.
* `name` - the name of the view to be deleted.

## Examples

The following command will drop the view named `recent_series`:

```yql
DROP VIEW recent_series;
```

## See also

* [CREATE VIEW](create-view.md)
* [ALTER VIEW](alter-view.md)
