### Examples

```yql
SELECT FormatType(CallableType(
  1, -- optional args count
  DataType("Double"), -- result type
  DataType("String"), -- arg #1 type
  OptionalType(DataType("Int64")) -- arg #2 type
)); -- Callable<(String,[Int64?])->Double>
```

## GenericType, UnitType, and VoidType {#generictype}

Return the same-name [special data types](../../types/special.md). They have no arguments because they are not parameterized.

### Examples

```yql
SELECT FormatType(VoidType()); -- Void
```
