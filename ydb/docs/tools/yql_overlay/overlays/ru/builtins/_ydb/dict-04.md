### Сигнатура

```yql
DictHasItems(Dict<K,V>)->Bool
DictHasItems(Dict<K,V>?)->Bool?
```

Проверка того, что словарь содержит хотя бы один элемент.

### Примеры

```yql
SELECT DictHasItems(AsDict(AsTuple(1, AsList("foo", "bar")))) FROM my_table;
```

{% if feature_column_container_type %}

```yql
SELECT DictHasItems(dict_column) FROM my_table;
```

{% endif %}
