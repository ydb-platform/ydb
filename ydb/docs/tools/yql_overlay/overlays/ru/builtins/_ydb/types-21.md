### Сигнатура

```yql
VariantUnderlyingType(VariantType)->низлежащий тип варианта
```

{% if feature_codegen %}Если этой функции передается тип, то она выполняет{% else %}Выполняет{% endif %} действие, обратное [VariantType](#varianttype) — возвращает низлежащий тип по типу варианта.

{% if feature_codegen %}

Если этой функции передается хендл типа, то она выполняет действие, обратное [VariantTypeHandle](#varianttypehandle) — возвращает хендл низлежащего типа по хендлу типа варианта.

{% endif %}

### Примеры

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

## Функции для работы с типами данных во время выполнения вычислений

Для работы с типами данных во время выполнения вычислений используется механизм хендлов типов - [ресурс](../../types/special.md), содержащий непрозрачное описание типа. После конструирования хендла типа можно вернуться к обычному типу с помощью функции [EvaluateType](#evaluatetype). Для отладки сконвертировать хендл типа в строку можно с помощью функции [FormatType](#formattype).

### TypeHandle

Получение хендла типа из типа, переданного в аргумент.

#### Сигнатура

```yql
TypeHandle(Type)->хэндл типа
```

#### Примеры

```yql
SELECT FormatType(TypeHandle(TypeOf("foo")));  -- String
```

### EvaluateType

#### Сигнатура

```yql
EvaluateType(TypeHandle)->тип
```

Получение типа из хендла типа, переданного в аргумент. Функция вычисляется до начала основного расчета, как и [EvaluateExpr](../basic.md#evaluate_expr_atom).

#### Примеры

```yql
SELECT FormatType(EvaluateType(TypeHandle(TypeOf("foo"))));  -- String
```

### ParseTypeHandle

#### Сигнатура

```yql
ParseTypeHandle(String)->хэндл типа
```

Построение хендла типа по строке с его описанием. [Документация по её формату](../../types/type_string.md).

#### Примеры

```yql
SELECT FormatType(ParseTypeHandle("List<Int32>"));  -- List<int32>
```

### TypeKind

#### Сигнатура

```yql
TypeKind(TypeHandle)->String
```

Получение названия верхнего уровня типа из хендла типа, переданного в аргумент.

#### Примеры

```yql
SELECT TypeKind(TypeHandle(TypeOf("foo")));  -- Data
SELECT TypeKind(ParseTypeHandle("List<Int32>"));  -- List
```

### DataTypeComponents

#### Сигнатура

```yql
DataTypeComponents(DataTypeHandle)->List<String>
```

Получение названия и параметров [примитивного типа данных](../../types/primitive.md) из хендла примитивного типа, переданного в аргумент. Обратная функция - [DataTypeHandle](#datatypehandle).

#### Примеры

```yql
SELECT DataTypeComponents(TypeHandle(TypeOf("foo")));  -- ["String"]
SELECT DataTypeComponents(ParseTypeHandle("Decimal(4,1)"));  -- ["Decimal", "4", "1"]
```

### DataTypeHandle

#### Сигнатура

```yql
DataTypeHandle(List<String>)->хэндл примитивного типа данных
```

Построение хендла [примитивного типа данных](../../types/primitive.md) из его названия и параметров, переданных списком в аргумент. Обратная функция - [DataTypeComponents](#datatypecomponents).

#### Примеры

```yql
SELECT FormatType(DataTypeHandle(
    AsList("String")
)); -- String

SELECT FormatType(DataTypeHandle(
    AsList("Decimal", "4", "1")
)); -- Decimal(4,1)
```

### OptionalTypeHandle

#### Сигнатура

```yql
OptionalTypeHandle(TypeHandle)->хэндл опционального типа
```

Добавляет в переданный хендл типа возможность содержать `NULL`.

#### Примеры

```yql
SELECT FormatType(OptionalTypeHandle(
    TypeHandle(DataType("Bool"))
)); -- Bool?
```

### PgTypeName

#### Сигнатура

```yql
PgTypeName(PgTypeHandle)->String
```

Получение имени PostgreSQL типа из хендла типа, переданного в аргумент. Обратная функция - [PgTypeHandle](#pgtypehandle).

#### Примеры

```yql
SELECT PgTypeName(ParseTypeHandle("pgint4")); -- int4
```

### PgTypeHandle

#### Сигнатура

```yql
PgTypeHandle(String)->хендл типа
```

Построение хендла типа по имени PostgreSQL типа, переданного в аргумент. Обратная функция - [PgTypeName](#pgtypename).

#### Примеры

```yql
SELECT FormatType(PgTypeHandle("int4")); -- pgint4
```

### ListTypeHandle и StreamTypeHandle {#list-stream-typehandle}

#### Сигнатура

```yql
ListTypeHandle(TypeHandle)->хэндл списочного типа
StreamTypeHandle(TypeHandle)->хэндл потокового типа
```

Строит хендл типа списка или потока по переданному хендлу типа элемента.

#### Примеры

```yql
SELECT FormatType(ListTypeHandle(
    TypeHandle(DataType("Bool"))
)); -- List<Bool>
```

### EmptyListTypeHandle и EmptyDictTypeHandle

#### Сигнатура

```yql
EmptyListTypeHandle()->хэндл типа пустого списка
EmptyDictTypeHandle()->хэндл типа пустого словаря
```

Строит хендл типа пустого списка или словаря.

#### Примеры

```yql
SELECT FormatType(EmptyListTypeHandle()); -- EmptyList
```

### TupleTypeComponents

#### Сигнатура

```yql
TupleTypeComponents(TupleTypeHandle)->List<TypeHandle>
```

Получение списка хендлов типов элементов из хендла типа кортежа, переданного в аргумент. Обратная функция - [TupleTypeHandle](#tupletypehandle).

#### Примеры

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

#### Сигнатура

```yql
TupleTypeHandle(List<TypeHandle>)->хэндл типа кортежа
```

Построение хендла типа кортежа из хендлов типов элементов, переданных списком в аргумент. Обратная функция - [TupleTypeComponents](#tupletypecomponents).

#### Примеры

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

#### Сигнатура

```yql
StructTypeComponents(StructTypeHandle)->List<Struct<Name:String, Type:TypeHandle>>
```

Получение списка хендлов типов элементов и их имен из хендла типа структуры, переданного в аргумент. Обратная функция - [StructTypeHandle](#structtypehandle).

#### Примеры

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

#### Сигнатура

```yql
StructTypeHandle(List<Struct<Name:String, Type:TypeHandle>>)->хэндл типа структуры
```

Построение хендла типа структуры из хендлов типов элементов и имен, переданных списком в аргумент. Обратная функция - [StructTypeComponents](#structtypecomponents).

#### Примеры

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

#### Сигнатура

```yql
DictTypeComponents(DictTypeHandle)->Struct<Key:TypeHandle, Payload:TypeHandle>
```

Получение хендла типа-ключа и хендла типа-значения - из хендла типа словаря, переданного в аргумент. Обратная функция - [DictTypeHandle](#dicttypehandle).

#### Примеры

```yql
$d = DictTypeComponents(ParseTypeHandle("Dict<Int32,String>"));

SELECT
    FormatType($d.Key),     -- Int32
    FormatType($d.Payload); -- String
```

### DictTypeHandle

#### Сигнатура

```yql
DictTypeHandle(TypeHandle, TypeHandle)->хэндл типа словаря
```

Построение хендла типа словаря из хендла типа-ключа и хендла типа-значения, переданных в аргументы. Обратная функция - [DictTypeComponents](#dicttypecomponents).

#### Примеры

```yql
SELECT FormatType(
    DictTypeHandle(
        ParseTypeHandle("Int32"),
        ParseTypeHandle("String")
    )
); -- Dict<Int32, String>
```

### ResourceTypeTag

#### Сигнатура

```yql
ResourceTypeTag(ResourceTypeHandle)->String
```

Получение тега из хендла типа ресурса, переданного в аргумент. Обратная функция - [ResourceTypeHandle](#resourcetypehandle).

#### Примеры

```yql
SELECT ResourceTypeTag(ParseTypeHandle("Resource<foo>")); -- foo
```

### ResourceTypeHandle

#### Сигнатура

```yql
ResourceTypeHandle(String)->хэндл типа ресурса
```

Построение хендла типа ресурса по значению тега, переданного в аргумент. Обратная функция - [ResourceTypeTag](#resourcetypetag).

#### Примеры

```yql
SELECT FormatType(ResourceTypeHandle("foo")); -- Resource<'foo'>
```

### TaggedTypeComponents

#### Сигнатура

```yql
TaggedTypeComponents(TaggedTypeHandle)->Struct<Base:TypeHandle, Tag:String>
```

Получение тега и базового типа из хендла декорированного типа, переданного в аргумент. Обратная функция - [TaggedTypeHandle](#taggedtypehandle).

#### Примеры

```yql
$t = TaggedTypeComponents(ParseTypeHandle("Tagged<Int32,foo>"));

SELECT FormatType($t.Base), $t.Tag; -- Int32, foo
```

### TaggedTypeHandle

#### Сигнатура

```yql
TaggedTypeHandle(TypeHandle, String)->хэндл декорированного типа
```

Построение хендла декорированного типа по хендлу базового типа и имени тега, переданных в аргументах. Обратная функция - [TaggedTypeComponents](#taggedtypecomponents).

#### Примеры

```yql
SELECT FormatType(TaggedTypeHandle(
    ParseTypeHandle("Int32"), "foo"
)); -- Tagged<Int32, 'foo'>
```

### VariantTypeHandle

#### Сигнатура

```yql
VariantTypeHandle(StructTypeHandle)->хэндл типа варианта над структурой
VariantTypeHandle(TupleTypeHandle)->хэндл типа варианта над кортежем
```

Построение хендла типа варианта по хендлу низлежащего типа, переданного в аргумент. Обратная функция - [VariantUnderlyingType](#variantunderlyingtype).

#### Примеры

```yql
SELECT FormatType(VariantTypeHandle(
    ParseTypeHandle("Tuple<Int32, String>")
)); -- Variant<Int32, String>
```

### VoidTypeHandle и NullTypeHandle

#### Сигнатура

```yql
VoidTypeHandle()->хэндл типа Void
NullTypeHandle()->хэндл типа Null
```

Построение хендла типов Void и Null соответственно.

#### Примеры

```yql
SELECT FormatType(VoidTypeHandle()); -- Void
SELECT FormatType(NullTypeHandle()); -- Null
```

### CallableTypeComponents

#### Сигнатура

```yql
CallableTypeComponents(CallableTypeHandle)->
Struct<
    Arguments:List<Struct<
        Flags:List<String>,
        Name:String,
        Type:TypeHandle>>,
    OptionalArgumentsCount:Uint32,
    Payload:String,
    Result:TypeHandle
>
```

Получение описания хендла типа вызываемого значения, переданного в аргумент. Обратная функция - [CallableTypeHandle](#callabletypehandle).

#### Примеры

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

#### Сигнатура

```yql
CallableArgument(TypeHandle, [String, [List<String>]])->Struct<Flags:List<String>,Name:String,Type:TypeHandle>
```

Упаковка в структуру описания аргумента вызываемого значения для передачи в функцию [CallableTypeHandle](#callabletypehandle) по следующим аргументам:

1. Хендл типа аргумента.
2. Необязательное имя аргумента. Значение по умолчанию - пустая строка.
3. Необязательные флаги аргумента в виде списка строк. Значение по умолчанию - пустой список. Поддерживаемые флаги - "AutoMap".

### CallableTypeHandle

#### Сигнатура

```yql
CallableTypeHandle(TypeHandle, List<Struct<Flags:List<String>,Name:String,Type:TypeHandle>>, [Uint32, [String]])->хэндл типа вызываемого значения
```

Построение хендла типа вызываемого значения по следующим аргументам:

1. Хендл типа возвращаемого значения.
2. Список описаний аргументов, полученных через функцию [CallableArgument](#callableargument).
3. Необязательное количество необязательных аргументов в вызываемом значении. Значение по умолчанию - 0.
4. Необязательная метка для типа вызываемого значения. Значение по умолчанию - пустая строка.

Обратная функция - [CallableTypeComponents](#callabletypecomponents).

#### Примеры

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

#### Сигнатура

```yql
LambdaArgumentsCount(LambdaFunction)->Uint32
```

Получение количества аргументов в лямбда-функции.

#### Примеры

```yql
SELECT LambdaArgumentsCount(($x, $y)->($x+$y))
; -- 2
```

### LambdaOptionalArgumentsCount

#### Сигнатура

```yql
LambdaOptionalArgumentsCount(LambdaFunction)->Uint32
```

Получение количества опциональных аргументов в лямбда-функции.

#### Примеры

```yql
SELECT LambdaOptionalArgumentsCount(($x, $y, $z?)->(if($x,$y,$z)))
; -- 1
```

{% endif %}
