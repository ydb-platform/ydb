{% if feature_codegen %}If a type is passed to this function, then it {% else %}Performs{% endif %} an action reverse to [VariantType](#varianttype): it returns the underlying type based on the variant type.

{% if feature_codegen %}

If a type handle is passed to this function, it performs the action reverse to [VariantTypeHandle](#varianttypehandle): returns the handle of the underlying type based on the handle of the variant type.

{% endif %}

### Examples

```yql
SELECT FormatType(VariantUnderlyingType(
  ParseType("Variant<foo:Int32,bar:Double>")
)), -- Struct<'bar':Double,'foo':Int32>
FormatType(VariantUnderlyingType(
  ParseType("Variant<Int32,Double>")
)); -- Tuple<Int32,Double>
```

{% if feature_codegen %}

```yql
SELECT FormatType(VariantUnderlyingType(
  ParseTypeHandle("Variant<foo:Int32,bar:Double>")
)), -- Struct<'bar':Double,'foo':Int32>
FormatType(VariantUnderlyingType(
  ParseTypeHandle("Variant<Int32,Double>")
)); -- Tuple<Int32,Double>
```

{% endif %}

{% if feature_codegen %}

## Functions for data types during calculations

To work with data types during calculations, use handle types: these are [resources](../../types/special.md) that contain an opaque type definition. After constructing the type handle, you can revert to the regular type using the [EvaluateType](#evaluatetype) function. For debug purposes, you can convert a handle type to a string using the [FormatType](#formattype) function.

### TypeHandle

Getting a type handle from the type passed to the argument.

#### Examples

```yql
SELECT FormatType(TypeHandle(TypeOf("foo")));  -- String
```

### EvaluateType

Getting the type from the type handle passed to the argument. The function is evaluated before the start of the main calculation, as well as [EvaluateExpr](../basic.md#evaluate_expr_atom).

#### Examples

```yql
SELECT FormatType(EvaluateType(TypeHandle(TypeOf("foo"))));  -- String
```

### ParseTypeHandle

Building a type handle from a string with description. [Documentation for its format](../../types/type_string.md).

#### Examples

```yql
SELECT FormatType(ParseTypeHandle("List<Int32>"));  -- List<int32>
```

### TypeKind

Getting the top-level type name from the type handle passed to the argument.

#### Examples

```yql
SELECT TypeKind(TypeHandle(TypeOf("foo")));  -- Data
SELECT TypeKind(ParseTypeHandle("List<Int32>"));  -- List
```

### DataTypeComponents

Getting the name and parameters for a [primitive data type](../../types/primitive.md) from the primitive type handle passed to the argument. Reverse function: [DataTypeHandle](#datatypehandle).

#### Examples

```yql
SELECT DataTypeComponents(TypeHandle(TypeOf("foo")));  -- ["String"]
SELECT DataTypeComponents(ParseTypeHandle("Decimal(4,1)"));  -- ["Decimal", "4", "1"]
```

### DataTypeHandle

Constructing a handle for a [primitive data type](../../types/primitive.md) from its name and parameters passed to the argument as a list. Reverse function: [DataTypeComponents](#datatypecomponents).

#### Examples

```yql
SELECT FormatType(DataTypeHandle(
    AsList("String")
)); -- String

SELECT FormatType(DataTypeHandle(
    AsList("Decimal", "4", "1")
)); -- Decimal(4,1)
```

### OptionalTypeHandle

Adds the option to assign `NULL` to the passed type handle.

#### Examples

```yql
SELECT FormatType(OptionalTypeHandle(
    TypeHandle(DataType("Bool"))
)); -- Bool?
```

### PgTypeName

Getting the name of the PostgreSQL type from the type handle passed to the argument. Inverse function: [PgTypeHandle](#pgtypehandle).

#### Examples

```yql
SELECT PgTypeName(ParseTypeHandle("pgint4")); -- int4
```

### PgTypeHandle

Builds a type handle based on the passed name of the PostgreSQL type. Inverse function: [PgTypeName](#pgtypename).

#### Examples

```yql
SELECT FormatType(PgTypeHandle("int4")); -- pgint4
```

### ListTypeHandle and StreamTypeHandle {#list-stream-typehandle}

Builds a list type handle or stream type handle based on the passed element type handle.

#### Examples

```yql
SELECT FormatType(ListTypeHandle(
    TypeHandle(DataType("Bool"))
)); -- List<Bool>
```

### EmptyListTypeHandle and EmptyDictTypeHandle

Constructs a handle for an empty list or dictionary.

#### Examples

```yql
SELECT FormatType(EmptyListTypeHandle()); -- EmptyList
```

### TupleTypeComponents

Getting a list of element type handles from the tuple type handle passed to the argument. Inverse function: [TupleTypeHandle](#tupletypehandle).

#### Examples

```yql
SELECT ListMap(
   TupleTypeComponents(
       ParseTypeHandle("Tuple<Int32, String>")
   ),
   ($x)->{
       return FormatType($x)
   }
); -- ["Int32", "String"]
```

### TupleTypeHandle

Building a tuple type handle from handles of element types passed as a list to the argument. Inverse function: [TupleTypeComponents](#tupletypecomponents).

#### Examples

```yql
SELECT FormatType(
    TupleTypeHandle(
        AsList(
            ParseTypeHandle("Int32"),
            ParseTypeHandle("String")
        )
    )
); -- Tuple<Int32,String>
```

### StructTypeComponents

Getting a list of element type handles and their names from the structure type handle passed to the argument. Inverse function: [StructTypeHandle](#structtypehandle).

#### Examples

```yql
SELECT ListMap(
    StructTypeComponents(
        ParseTypeHandle("Struct<a:Int32, b:String>")
    ),
    ($x) -> {
        return AsTuple(
            FormatType($x.Type),
            $x.Name
        )
    }
); -- [("Int32","a"), ("String","b")]
```

### StructTypeHandle

Building a structure type handle from handles of element types and names passed as a list to the argument. Inverse function: [StructTypeComponents](#structtypecomponents).

#### Examples

```yql
SELECT FormatType(
    StructTypeHandle(
        AsList(
            AsStruct(ParseTypeHandle("Int32") as Type,"a" as Name),
            AsStruct(ParseTypeHandle("String") as Type, "b" as Name)
        )
    )
); -- Struct<'a':Int32,'b':String>
```

### DictTypeComponents

Getting a key-type handle and a value-type handle from the dictionary-type handle passed to the argument. Inverse function: [DictTypeHandle](#dicttypehandle).

#### Examples

```yql
$d = DictTypeComponents(ParseTypeHandle("Dict<Int32,String>"));

SELECT
    FormatType($d.Key),     -- Int32
    FormatType($d.Payload); -- String
```

### DictTypeHandle

Building a dictionary-type handle from a key-type handle and a value-type handle passed to arguments. Inverse function: [DictTypeComponents](#dicttypecomponents).

#### Examples

```yql
SELECT FormatType(
    DictTypeHandle(
        ParseTypeHandle("Int32"),
        ParseTypeHandle("String")
    )
); -- Dict<Int32, String>
```

### ResourceTypeTag

Getting the tag from the resource type handle passed to the argument. Inverse function: [ResourceTypeHandle](#resourcetypehandle).

#### Examples

```yql
SELECT ResourceTypeTag(ParseTypeHandle("Resource<foo>")); -- foo
```

### ResourceTypeHandle

Building a resource-type handle from the tag value passed to the argument. Inverse function: [ResourceTypeTag](#resourcetypetag).

#### Examples

```yql
SELECT FormatType(ResourceTypeHandle("foo")); -- Resource<'foo'>
```

### TaggedTypeComponents

Getting the tag and the basic type from the decorated type handle passed to the argument. Inverse function: [TaggedTypeHandle](#taggedtypehandle).

#### Examples

```yql
$t = TaggedTypeComponents(ParseTypeHandle("Tagged<Int32,foo>"));

SELECT FormatType($t.Base), $t.Tag; -- Int32, foo
```

### TaggedTypeHandle

Constructing a decorated type handle based on the base type handle and the tag name passed in arguments. Inverse function: [TaggedTypeComponents](#taggedtypecomponents).

#### Examples

```yql
SELECT FormatType(TaggedTypeHandle(
    ParseTypeHandle("Int32"), "foo"
)); -- Tagged<Int32, 'foo'>
```

### VariantTypeHandle

Building a variant-type handle from the handle of the underlying type passed to the argument. Inverse function: [VariantUnderlyingType](#variantunderlyingtype).

#### Examples

```yql
SELECT FormatType(VariantTypeHandle(
    ParseTypeHandle("Tuple<Int32, String>")
)); -- Variant<Int32, String>
```

### VoidTypeHandle and NullTypeHandle

Constructing a handle for Void and Null types, respectively.

#### Examples

```yql
SELECT FormatType(VoidTypeHandle()); -- Void
SELECT FormatType(NullTypeHandle()); -- Null
```

### CallableTypeComponents

Getting the handle description for the type of callable value passed to the argument. Inverse function: [CallableTypeHandle](#callabletypehandle).

#### Examples

```yql
$formatArgument = ($x) -> {
    return AsStruct(
        FormatType($x.Type) as Type,
        $x.Name as Name,
        $x.Flags as Flags
    )
};

$formatCallable = ($x) -> {
    return AsStruct(
        $x.OptionalArgumentsCount as OptionalArgumentsCount,
        $x.Payload as Payload,
        FormatType($x.Result) as Result,
        ListMap($x.Arguments, $formatArgument) as Arguments
    )
};

SELECT $formatCallable(
    CallableTypeComponents(
        ParseTypeHandle("(Int32,[bar:Double?{Flags:AutoMap}])->String")
    )
);  -- (OptionalArgumentsCount: 1, Payload: "", Result: "String", Arguments: [
    --   (Type: "Int32", Name: "", Flags: []),
    --   (Type: "Double?", Name: "bar", Flags: ["AutoMap"]),
    -- ])
```

### CallableArgument

Packing  the description of the argument of the callable value into the structure to be passed to the [CallableTypeHandle](#callabletypehandle) function with the following arguments:

1. Argument type handle.
2. Optional argument name. The default value is an empty string.
3. A list of strings with optional argument flags. The default value is an empty list. Supported flags are "AutoMap".

### CallableTypeHandle

Constructing the type handle of the called value using the following arguments:

1. Handle of the return value type.
2. List of descriptions of arguments received using the [CallableArgument](#callableargument) function.
3. Optional number of optional arguments in the callable value. The default value is 0.
4. An optional label for the called value type. The default value is an empty string.

Inverse function: [CallableTypeComponents](#callabletypecomponents).

#### Examples

```yql
SELECT FormatType(
    CallableTypeHandle(
        ParseTypeHandle("String"),
        AsList(
            CallableArgument(ParseTypeHandle("Int32")),
            CallableArgument(ParseTypeHandle("Double?"), "bar", AsList("AutoMap"))
        ),
        1
    )
);  -- Callable<(Int32,['bar':Double?{Flags:AutoMap}])->String>
```

### LambdaArgumentsCount

Getting the number of arguments in a lambda function.

#### Examples

```yql
SELECT LambdaArgumentsCount(($x, $y)->($x+$y))
; -- 2
```

{% endif %}
