## Variant, AsVariant {#variant}

`Variant()` creates a variant value over a tuple or structure.

Arguments:

* Value
* String with a field name or tuple index
* Variant type

**Example**

```yql
$var_type = Variant<foo: Int32, bar: Bool>;

SELECT
   Variant(6, "foo", $var_type) as Variant1Value,
   Variant(false, "bar", $var_type) as Variant2Value;
```

`AsVariant()` creates a value of a [variant over a structure](../../../types/containers.md) including one field. This value can be implicitly converted to any variant over a structure that has a matching data type for this field name and might include more fields with other names.

Arguments:

* Value
* A string with the field name

**Example**

```yql
SELECT
   AsVariant(6, "foo") as VariantValue
```

