### Сигнатуры

```yql
Pickle(T)->String
StablePickle(T)->String
Unpickle(Type<T>, String)->T
```

### Примеры

{% if feature_tablesample==true %}
```yql
SELECT *
FROM my_table
WHERE Digest::MurMurHash32(
        Pickle(TableRow())
    ) % 10 == 0; -- в реальности лучше использовать TABLESAMPLE

$buf = Pickle(123);
SELECT Unpickle(Int32, $buf);
```
{% else %}
```yql
SELECT *
FROM my_table
WHERE Digest::MurMurHash32(
        Pickle(TableRow())
    ) % 10 == 0;

$buf = Pickle(123);
SELECT Unpickle(Int32, $buf);
```
{% endif %}
