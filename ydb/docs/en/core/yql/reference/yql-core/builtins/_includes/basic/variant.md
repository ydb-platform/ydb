## Variant {#variant}

`Variant()` creates a variant value over a tuple or structure.

Arguments:

* Value
* String with a field name or tuple index
* Variant type

### Example

```yql
$var_type = Variant<foo: Int32, bar: Bool>;

SELECT
   Variant(6, "foo", $var_type) as Variant1Value,
   Variant(false, "bar", $var_type) as Variant2Value;
```

## AsVariant {#asvariant}

`AsVariant()` creates a value of a [variant over a structure](../../../types/containers.md) including one field. This value can be implicitly converted to any variant over a structure that has a matching data type for this field name and might include more fields with other names.

Arguments:

* Value
* A string with the field name

### Example

```yql
SELECT
   AsVariant(6, "foo") as VariantValue
```

## Visit, VisitOrDefault {#visit}

Processes the possible values of a variant over a structure or tuple using the provided handler functions for each field/element of the variant.

### Signature

```yql
Visit(Variant<key1: K1, key2: K2, ...>, K1->R AS key1, K2->R AS key2, ...)->R
Visit(Variant<K1, K2, ...>, K1->R, K2->R, ...)->R

VisitOrDefault(Variant<K1, K2, ...>{Flags:AutoMap}, R, [K1->R, [K2->R, ...]])->R
VisitOrDefault(Variant<key1: K1, key2: K2, ...>{Flags:AutoMap}, R, [K1->R AS key1, [K2->R AS key2, ...]])->R
```

### Arguments

* For a variant over structure: accepts the variant as the positional argument and named arguments (handlers) corresponding to each field of the variant.
* For a variant over tuple: accepts the variant and handlers for each element of the variant as positional arguments.
* `VisitOrDefault` includes an additional positional argument (on the second place) for the default value, enabling the omission of certain handlers.

### Example

```yql
$vartype = Variant<num: Int32, flag: Bool, str: String>;
$handle_num = ($x) -> { return 2 * $x; };
$handle_flag = ($x) -> { return If($x, 200, 10); };
$handle_str = ($x) -> { return Unwrap(CAST(LENGTH($x) AS Int32)); };

$visitor = ($var) -> { return Visit($var, $handle_num AS num, $handle_flag AS flag, $handle_str AS str); };
SELECT
    $visitor(Variant(5, "num", $vartype)),                -- 10
    $visitor(Just(Variant(True, "flag", $vartype))),      -- Just(200)
    $visitor(Just(Variant("somestr", "str", $vartype))),  -- Just(7)
    $visitor(Nothing(OptionalType($vartype))),            -- Nothing(Optional<Int32>)
    $visitor(NULL)                                        -- NULL
;
```

## VariantItem {#variantitem}

Returns the value of a homogeneous variant (i.e., a variant containing fields/elements of the same type).

### Signature

```yql
VariantItem(Variant<key1: K, key2: K, ...>{Flags:AutoMap})->K
VariantItem(Variant<K, K, ...>{Flags:AutoMap})->K
```

### Example

```yql
$vartype1 = Variant<num1: Int32, num2: Int32, num3: Int32>;
SELECT
    VariantItem(Variant(7, "num2", $vartype1)),          -- 7
    VariantItem(Just(Variant(5, "num1", $vartype1))),    -- Just(5)
    VariantItem(Nothing(OptionalType($vartype1))),       -- Nothing(Optional<Int32>)
    VariantItem(NULL)                                    -- NULL
;
```

