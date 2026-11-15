### Сигнатура

```yql
OptionalItemType(OptionalType)->тип элемента опционального типа
ListItemType(ListType)->тип элемента списочного типа
StreamItemType(StreamType)->тип элемента потокового типа
```

{% if feature_codegen %} Если этим функциям передается тип, то они выполняют{% else %}Выполняют{% endif %} действие, обратное [OptionalType](#optionaltype), [ListType](#listtype) и [StreamType](#listtype) — возвращают тип элемента по типу соответствующего контейнера.

{% if feature_codegen %}

Если этим функциям передается хендл типа, то выполняют действие, обратное [OptionalTypeHandle](#optionaltypehandle), [ListTypeHandle](#list-stream-typehandle) и [StreamTypeHandle](#list-stream-typehandle) - возвращают хендл типа элемента по хендлу типа соответствующего контейнера.

{% endif %}

### Примеры

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
