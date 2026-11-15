### Сигнатура

```yql
DictContains(Dict<K,V>, K)->Bool
DictContains(Dict<K,V>?, K)->Bool
DictContains(Dict<K,V>, K?)->Bool
DictContains(Dict<K,V>?, K?)->Bool
```

Проверка наличия элемента в словаре по ключу. Возвращает true или false.

### Примеры

```yql
SELECT DictContains(AsDict(
    AsTuple(1, AsList("foo", "bar")),
    AsTuple(2, AsList("bar", "baz"))
), 42);
-- false
```

{% if feature_column_container_type %}

```yql
SELECT DictContains(dict_column, "foo")
FROM my_table;
```

{% endif %}
