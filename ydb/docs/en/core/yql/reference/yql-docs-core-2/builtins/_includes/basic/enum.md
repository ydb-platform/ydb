## Enum, AsEnum {#enum}

`Enum()` creates an enumeration value.

Arguments:

* A string with the field name
* Enumeration type

**Example**

```yql
$enum_type = Enum<Foo, Bar>;
SELECT
   Enum("Foo", $enum_type) as Enum1Value,
   Enum("Bar", $enum_type) as Enum2Value;
```

`AsEnum()` creates a value of [enumeration](../../../types/containers.md) including one element. This value can be implicitly cast to any enumeration containing such a name.

Arguments:

* A string with the name of an enumeration item

**Example**

```yql
SELECT
   AsEnum("Foo");
```

