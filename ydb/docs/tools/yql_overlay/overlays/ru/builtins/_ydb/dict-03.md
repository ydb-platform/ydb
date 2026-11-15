### Сигнатура

```yql
DictLength(Dict<K,V>)->Uint64
DictLength(Dict<K,V>?)->Uint64?
```

Количество элементов в словаре.

### Примеры

```yql
SELECT DictLength(AsDict(AsTuple(1, AsList("foo", "bar"))));
```

{% if feature_column_container_type %}

```yql
SELECT DictLength(dict_column) FROM my_table;
```

{% endif %}
