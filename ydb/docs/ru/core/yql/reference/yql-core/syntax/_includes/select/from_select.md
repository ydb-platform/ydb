
# FROM ... SELECT ...

Перевернутая форма записи, в которой сначала указывается источник данных, а затем — операция.

**Примеры**

``` yql
FROM my_table SELECT key, value;
```

``` yql
FROM a_table AS a
JOIN b_table AS b
USING (key)
SELECT *;
```