### Signature


```yql
MIN(T?)->T?
MIN(T)->T?
MAX(T?)->T?
MAX(T)->T?
```


Minimum or maximum value.

As an argument, you may use an arbitrary computable expression with a result that allows value comparison.

### Examples


```yql
SELECT MIN(value), MAX(value) FROM my_table;
```
