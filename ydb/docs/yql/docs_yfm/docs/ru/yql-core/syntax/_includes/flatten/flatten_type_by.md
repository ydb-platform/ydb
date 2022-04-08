### Уточнение типа контейнера {#flatten-by-specific-type}

Чтобы уточнить тип контейнера, по которому необходимо произвести преобразование, можно использовать:

* `FLATTEN LIST BY`

   Для `Optional<List<T>>` операция `FLATTEN LIST BY` будет разворачивать список, интерпретируя `NULL`-значение как пустой список.
* `FLATTEN DICT BY`

   Для `Optional<Dict<T>>` операция `FLATTEN DICT BY` будет разворачивать словарь, интерпретируя `NULL`-значение как пустой словарь.
* `FLATTEN OPTIONAL BY`

   Чтобы фильтровать `NULL`-значения без размножения, необходимо уточнить операцию до `FLATTEN OPTIONAL BY`.

**Примеры**

```sql
SELECT
  t.item.0 AS key,
  t.item.1 AS value,
  t.dict_column AS original_dict,
  t.other_column AS other
FROM my_table AS t
FLATTEN DICT BY dict_column AS item;
```

```sql
SELECT * FROM (
    SELECT
        AsList(1, 2, 3) AS a,
        AsList("x", "y", "z") AS b
) FLATTEN LIST BY (a, b);
```

``` yql
SELECT * FROM (
    SELECT
        "1;2;3" AS a,
        AsList("x", "y", "z") AS b
) FLATTEN LIST BY (String::SplitToList(a, ";") as a, b);
```