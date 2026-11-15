### Signature


```yql
SUM(Unsigned?)->Uint64?
SUM(Signed?)->Int64?
SUM(Interval?)->Interval?
SUM(Decimal(N, M)?)->Decimal(35, M)?
```


Sum of the numbers.

As an argument, you may use an arbitrary computable expression with a numeric result or type `Interval`.

Integers are automatically expanded to 64 bits to reduce the risk of overflow.


```yql
SELECT SUM(value) FROM my_table;
```
