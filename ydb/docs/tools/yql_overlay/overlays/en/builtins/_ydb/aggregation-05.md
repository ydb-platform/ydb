### Signature


```yql
AVG(Double?)->Double?
AVG(Interval?)->Interval?
AVG(Decimal(N, M)?)->Decimal(N, M)?
```


Arithmetic average.

As an argument, you may use an arbitrary computable expression with a numeric result or type `Interval`.

Integer values and time intervals are automatically converted to Double.

### Examples


```yql
SELECT AVG(value) FROM my_table;
```
