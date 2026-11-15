### Сигнатура

```yql
DictKeys(Dict<K,V>)->List<K>
DictKeys(Dict<K,V>?)->List<K>?
```

Получение списка ключей словаря.

### Примеры

```yql
SELECT DictKeys(AsDict(AsTuple(1, AsList("foo", "bar"))));
-- [ 1 ]
```

{% if feature_column_container_type %}

```yql
SELECT DictKeys(dict_column)
FROM my_table;
```

{% endif %}
