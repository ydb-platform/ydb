### Signature


```yql
COUNT(*)->Uint64
COUNT(T)->Uint64
COUNT(T?)->Uint64
```


Counting the number of rows in a row or columnar table (if `*` or constant is specified as the argument) or non-empty values in a table column (if the column name is specified as an argument).

Like other aggregate functions, it can be combined with [GROUP BY](../../syntax/select/group-by.md) to get statistics on the parts of the table that correspond to the values in the columns being grouped. Use the modifier [DISTINCT](../../syntax/select/group-by.md#distinct) to count distinct values.

### Examples


```yql
SELECT COUNT(*) FROM my_table;
```
