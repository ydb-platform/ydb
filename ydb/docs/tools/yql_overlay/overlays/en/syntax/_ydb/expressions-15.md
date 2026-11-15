### Examples

```yql
SELECT
  t.struct.member,
  t.tuple.7,
  t.dict["key"],
  t.list[7]
FROM my_table AS t;
```

```yql
SELECT
  Sample::ReturnsStruct().member;
```
