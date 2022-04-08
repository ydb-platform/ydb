## Just, Unwrap, Nothing {#optional-ops}

`Just()`: Change the value's data type to [optional](../../../types/optional.md) from the current data type (i.e.,`T` is converted to `T?`).

The reverse operation is [Unwrap](#unwrap).

**Examples**

```yql
SELECT
  Just("my_string"); --  String?
```

`Unwrap()`: Converting the [optional](../../../types/optional.md) value of the data type to the relevant non-optional type, raising a runtime error if the data is `NULL`. This means that `T?` becomes `T`.

If the value isn't [optional](../../../types/optional.md), then the function returns its first argument unchanged.

Arguments:

1. Value to be converted.
2. An optional string with a comment for the error text.

Reverse operation is [Just](#just).

**Examples**

```yql
$value = Just("value");

SELECT Unwrap($value, "Unexpected NULL for $value");
```

`Nothing()`: Create an empty value for the specified [Optional](../../../types/optional.md) data type.

**Examples**

```yql
SELECT
  Nothing(String?); -- an empty (NULL) value with the String? type
```

[Learn more about ParseType and other functions for data types](../../types.md).

