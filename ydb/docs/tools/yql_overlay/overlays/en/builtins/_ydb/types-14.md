{% if feature_codegen %} If a type is passed to these functions, then they perform {% else %}Perform{% endif %} the action reverse to [OptionalType](#optionaltype), [ListType](#listtype), and [StreamType](#listtype): return the item type based on its container type.

{% if feature_codegen %}

If a type handle is passed to these functions, then they perform the action reverse to [OptionalTypeHandle](#optionaltypehandle), [ListTypeHandle](#list-stream-typehandle), and [StreamTypeHandle](#list-stream-typehandle): they return the handle of the element type based on the type handle of its container.

{% endif %}

### Examples

```yql
SELECT FormatType(ListItemType(
  ParseType("List<Int32>")
)); -- Int32
```

{% if feature_codegen %}

```yql
SELECT FormatType(ListItemType(
  ParseTypeHandle("List<Int32>")
)); -- Int32
```

{% endif %}
