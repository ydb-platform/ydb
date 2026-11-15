### Сигнатура

```yql
DictPayloads(Dict<K,V>)->List<V>
DictPayloads(Dict<K,V>?)->List<V>?
```

Получение списка значений словаря.

### Примеры

```yql
SELECT DictPayloads(AsDict(AsTuple(1, AsList("foo", "bar"))));
-- [ [ "foo", "bar" ] ]
```

{% if feature_column_container_type %}

```yql
SELECT DictPayloads(dict_column)
FROM my_table;
```

{% endif %}
