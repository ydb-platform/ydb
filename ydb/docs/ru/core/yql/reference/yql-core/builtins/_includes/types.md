# Функции для работы с типами данных

Помимо обычных функций, которые работают с конкретными значениями (типа FIND, COALESCE), YQL поддерживает функции для работами с [типами](../../types/index.md).
Функции позволяют узнать тип произвольного выражения, проанализировать контейнерный тип и создавть новый контейнерный тип на основе имеющегося.

**Примеры**
``` yql
$itemType = TypeOf($item);
SELECT CAST($foo AS ListType($itemType));  -- каст $foo к типу List<$itemType>
```

## FormatType {#formattype}

**Сигнатура**
```
FormatType(Type)->String
FormatType(TypeHandle)->String
```

Сериализация типа {% if feature_codegen %} или хендла типа{% endif %} в человекочитаемую строку. Это полезно для отладки, а также будет использоваться в последующих примерах данного раздела. [Документация по формату](../../types/type_string.md).

## FormatTypeDiff и FormatTypeDiffPretty {#formattypediff}

**Сигнатура**
```
FormatTypeDiff(Type, Type)->String
FormatTypeDiff(TypeHandle, TypeHandle)->String

FormatTypeDiffPretty(Type, Type)->String
FormatTypeDiffPretty(TypeHandle, TypeHandle)->String
```

Получение строкового представления разницы двух типов или двух хендлов типов. Pretty-версия делает результирующую строку более читаемой путем добавления переводов строк и пробелов.

## ParseType {#parsetype}

**Сигнатура**
```
ParseType(String)->Type
```
Построение типа по строке с его описанием. [Документация по её формату](../../types/type_string.md).

**Примеры**
``` yql
SELECT FormatType(ParseType("List<Int32>"));  -- List<int32>
```

## TypeOf {#typeof}

**Сигнатура**
```
TypeOf(<any expression>)->Type
```
Получение типа значения, переданного в аргумент.

**Примеры**
``` yql
SELECT FormatType(TypeOf("foo"));  -- String
```
``` yql
SELECT FormatType(TypeOf(AsTuple(1, 1u))); -- Tuple<Int32,Uint32>
```

## InstanceOf {#instanceof}

**Сигнатура**
```
InstanceOf(Type)->объект типа Type
```

Возвращает экземпляр объекта указанного типа. Полученный объект не имеет какого-то определенного значения.
InstanceOf можно использовать только в том случае, если результат выражения в котором InstanceOf используется зависит от типа InstanceOf, но не от значения.
В противном случае операция будет завершена с ошибкой.

**Примеры**
``` yql
SELECT InstanceOf(ParseType("Int32")) + 1.0; -- ошибка (Can't execute InstanceOf): результат зависит от (неопределенного) значения InstanceOf
SELECT FormatType(TypeOf(
    InstanceOf(ParseType("Int32")) +
    InstanceOf(ParseType("Double"))
)); -- вернет Double, так как сложение Int32 и Double возвращает Double (InstanceOf используется в контексте, где важен только его тип, но не значение)
```

## DataType {#datatype}

**Сигнатура**
```
DataType(String, [String, ...])->Type
```
Возвращает тип для [примитивных типов данных](../../types/primitive.md) по его имени.
Для некоторых типов (например Decimal) необходимо передавать параметры типа в качестве дополнительных аргументов.

**Примеры**
``` yql
SELECT FormatType(DataType("Bool")); -- Bool
SELECT FormatType(DataType("Decimal","5","1")); -- Decimal(5,1)
```

## OptionalType {#optionaltype}

**Сигнатура**
```
OptionalType(Type)->опциональный Type
```
Добавляет в переданный тип возможность содержать `NULL`.

**Примеры**
``` yql
SELECT FormatType(OptionalType(DataType("Bool"))); -- Bool?
SELECT FormatType(OptionalType(ParseType("List<String?>"))); -- List<String?>?
```

## ListType и StreamType {#listtype}

**Сигнатура**
```
ListType(Type)->тип списка с элементами типа Type
StreamType(Type)->тип потока с элементами типа Type
```

Строит тип списка или потока по переданному типу элемента.

**Примеры**
``` yql
SELECT FormatType(ListType(DataType("Bool"))); -- List<Bool>
```

## DictType {#dicttype}

**Сигнатура**
```
DictType(Type, Type)->тип словаря
```

Строит тип словаря по переданным типам ключа (первый аргумент) и значения (второй аргумент).

**Примеры**
``` yql
SELECT FormatType(DictType(
    DataType("String"),
    DataType("Double")
)); -- Dict<String,Double>
```

## TupleType {#tupletype}

**Сигнатура**
```
TupleType(Type, ...)->тип кортежа
```
Строит тип кортежа по переданным типам элементов.

**Примеры**
``` yql
SELECT FormatType(TupleType(
    DataType("String"),
    DataType("Double"),
    OptionalType(DataType("Bool"))
)); -- Tuple<String,Double,Bool?>
```

## StructType {#structtype}

**Сигнатура**
```
StructType(Type AS ElementName1, Type AS ElementName2, ...)->тип структуры
```
Строит тип структуры по переданным типам элементов. Для указания имен элементов используется стандартный синтаксис именованных аргументов.

**Примеры**
``` yql
SELECT FormatType(StructType(
    DataType("Bool") AS MyBool,
    ListType(DataType("String")) AS StringList
)); -- Struct<'MyBool':Bool,'StringList':List<String>>
```

## VariantType {#varianttype}

**Сигнатура**
```
VariantType(StructType)->тип варианта над структурой
VariantType(TupleType)->тип варианта над кортежем
```
Возвращает тип варианта по низлежащему типу (структуры или кортежа).

**Примеры**
``` yql
SELECT FormatType(VariantType(
  ParseType("Struct<foo:Int32,bar:Double>")
)); -- Variant<'bar':Double,'foo':Int32>
```

## ResourceType {#resourcetype}

**Сигнатура**
```
ResourceType(String)->тип ресурса
```
Возвращает тип [ресурса](../../types/special.md) по переданной строковой метке.

**Примеры**
``` yql
SELECT FormatType(ResourceType("Foo")); -- Resource<'Foo'>
```

## CallableType {#callabletype}

**Сигнатура**
```
CallableType(Uint32, Type, [Type, ...])->тип вызываемого значения
```
Строит тип вызываемого значения по следующим аргументам:

1. Число опциональных аргументов (если все обязательные — 0).
2. Тип результата.
3. Все последующие аргументы CallableType трактуются как типы аргументов вызываемого значения со сдвигом на два обязательных (например, третий аргумент CallableType описывает тип первого аргумента вызываемого значения).

**Примеры**
``` yql
SELECT FormatType(CallableType(
  1, -- optional args count
  DataType("Double"), -- result type
  DataType("String"), -- arg #1 type
  OptionalType(DataType("Int64")) -- arg #2 type
)); -- Callable<(String,[Int64?])->Double>
```

## GenericType, UnitType и VoidType {#generictype}

**Сигнатура**
```
GenericType()->тип
UnitType()->тип
VoidType()->тип
```
Возвращают одноименные [специальные типы данных](../../types/special.md). Аргументов нет, так как они не параметризуются.

**Примеры**
``` yql
SELECT FormatType(VoidType()); -- Void
```

## OptionalItemType, ListItemType и StreamItemType {#optionalitemtype}

**Сигнатура**
```
OptionalItemType(OptionalType)->тип элемента опционального типа
ListItemType(ListType)->тип элемента списочного типа
StreamItemType(StreamType)->тип элемента потокового типа
```

{% if feature_codegen %} Если этим функциям передается тип, то они выполняют{% else %}Выполняют{% endif %} действие, обратное [OptionalType](#optionaltype), [ListType](#listtype) и [StreamType](#streamtype) — возвращают тип элемента по типу соответствующего контейнера.
{% if feature_codegen %}
Если этим функциям передается хендл типа, то выполняют действие, обратное [OptionalTypeHandle](#optionaltypehandle), [ListTypeHandle](#listtypehandle) и [StreamTypeHandle](#streamtypehandle) - возвращают хендл типа элемента по хендлу типа соответствующего контейнера.
{% endif %}

**Примеры**
``` yql
SELECT FormatType(ListItemType(
  ParseType("List<Int32>")
)); -- Int32
```
{% if feature_codegen %}
``` yql
SELECT FormatType(ListItemType(
  ParseTypeHandle("List<Int32>")
)); -- Int32
```
{% endif %}

## DictKeyType и DictPayloadType {#dictkeytype}

**Сигнатура**
```
DictKetType(DictType)->тип ключа словаря
DictPayloadType(DictType)->тип значения словаря
```
Возвращают тип ключа или значения по типу словаря.

**Примеры**
``` yql
SELECT FormatType(DictKeyType(
  ParseType("Dict<Int32,String>")
)); -- Int32
```

## TupleElementType {#tupleelementtype}

**Сигнатура**
```
TupleElementType(TupleType, String)->тип элемента кортежа
```
Возвращает тип элемента кортежа по типу кортежа и индексу элемента (индекс с нуля).

**Примеры**
``` yql
SELECT FormatType(TupleElementType(
  ParseType("Tuple<Int32,Double>"), "1"
)); -- Double
```

## StructMemberType {#structmembertype}

**Сигнатура**
```
StructMemberType(StructType, String)->тип элемента структуры
```
Возвращает тип элемента структуры по типу структуры и имени элемента.

**Примеры**
``` yql
SELECT FormatType(StructMemberType(
  ParseType("Struct<foo:Int32,bar:Double>"), "foo"
)); -- Int32
```

## CallableResultType и CallableArgumentType {#callableresulttype}

**Сигнатура**
```
CallableResultType(CallableType)->тип результата вызываемого значения
CallableArgumentType(CallableType, Uint32)->тип аругмента вызываемого значения
```
`CallableResultType` возвращает тип результата по типу вызываемого значения, а `CallableArgumentType` — тип аргумента по типу вызываемого значения и его индексу (индекс с нуля).

**Примеры**
``` yql
$callable_type = ParseType("(String,Bool)->Double");

SELECT FormatType(CallableResultType(
    $callable_type
)), -- Double
FormatType(CallableArgumentType(
    $callable_type, 1
)); -- Bool
```

## VariantUnderlyingType {#variantunderlyingtype}

**Сигнатура**
```
VariantUnderlyingType(VariantType)->низлежащий тип варианта
```
{% if feature_codegen %}Если этой функции передается тип, то она выполняет{% else %}Выполняет{% endif %} действие, обратное [VariantType](#varianttype) — возвращает низлежащий тип по типу варианта.
{% if feature_codegen %}
Если этой функции передается хендл типа, то она выполняет действие, обратное [VariantTypeHandle](#varianttypehandle) — возвращает хендл низлежащего типа по хендлу типа варианта.
{% endif %}

**Примеры**
``` yql
SELECT FormatType(VariantUnderlyingType(
  ParseType("Variant<foo:Int32,bar:Double>")
)), -- Struct<'bar':Double,'foo':Int32>
FormatType(VariantUnderlyingType(
  ParseType("Variant<Int32,Double>")
)); -- Tuple<Int32,Double>
```
{% if feature_codegen %}
``` yql
SELECT FormatType(VariantUnderlyingType(
  ParseTypeHandle("Variant<foo:Int32,bar:Double>")
)), -- Struct<'bar':Double,'foo':Int32>
FormatType(VariantUnderlyingType(
  ParseTypeHandle("Variant<Int32,Double>")
)); -- Tuple<Int32,Double>
```
{% endif %}

{% if feature_codegen %}
# Функции для работы с типами данных во время выполнения вычислений

Для работы с типами данных во время выполнения вычислений используется механизм хендлов типов - [ресурс](../../types/special.md), содержащий непрозрачное описание типа. После конструирования хендла типа можно вернуться к обычному типу с помощью функции [EvaluateType](#evaluatetype). Для отладки сконвертировать хендл типа в строку можно с помощью функции [FormatType](#formattype).

## TypeHandle

Получение хендла типа из типа, переданного в аргумент.

**Сигнатура**
```
TypeHandle(Type)->хэндл типа
```

**Примеры:**
``` yql
SELECT FormatType(TypeHandle(TypeOf("foo")));  -- String
```
## EvaluateType

**Сигнатура**
```
EvaluateType(TypeHandle)->тип
```
Получение типа из хендла типа, переданного в аргумент. Функция вычисляется до начала основного расчета, как и [EvaluateExpr](../basic.md#evaluate_expr_atom).

**Примеры:**
``` yql
SELECT FormatType(EvaluateType(TypeHandle(TypeOf("foo"))));  -- String
```

## ParseTypeHandle

**Сигнатура**
```
ParseTypeHandle(String)->хэндл типа
```
Построение хендла типа по строке с его описанием. [Документация по её формату](../../types/type_string.md).

**Примеры:**
``` yql
SELECT FormatType(ParseTypeHandle("List<Int32>"));  -- List<int32>
```

## TypeKind

**Сигнатура**
```
TypeKind(TypeHandle)->String
```
Получение названия верхнего уровня типа из хендла типа, переданного в аргумент.

**Примеры:**
``` yql
SELECT TypeKind(TypeHandle(TypeOf("foo")));  -- Data
SELECT TypeKind(ParseTypeHandle("List<Int32>"));  -- List
```

## DataTypeComponents

**Сигнатура**
```
DataTypeComponents(DataTypeHandle)->List<String>
```
Получение названия и параметров [примитивного типа данных](../../types/primitive.md) из хендла примитивного типа, переданного в аргумент. Обратная функция - [DataTypeHandle](#datatypehandle).

**Примеры:**
``` yql
SELECT DataTypeComponents(TypeHandle(TypeOf("foo")));  -- ["String"]
SELECT DataTypeComponents(ParseTypeHandle("Decimal(4,1)"));  -- ["Decimal", "4", "1"]
```

## DataTypeHandle

**Сигнатура**
```
DataTypeHandle(List<String>)->хэндл примитивного типа данных
```
Построение хендла [примитивного типа данных](../../types/primitive.md) из его названия и параметров, переданных списком в аргумент. Обратная функция - [DataTypeComponents](#datatypecomponents).

**Примеры:**
``` yql
SELECT FormatType(DataTypeHandle(
    AsList("String")
)); -- String

SELECT FormatType(DataTypeHandle(
    AsList("Decimal", "4", "1")
)); -- Decimal(4,1)
```

## OptionalTypeHandle

**Сигнатура**
```
OptionalTypeHandle(TypeHandle)->хэндл опционального типа
```
Добавляет в переданный хендл типа возможность содержать `NULL`.

**Примеры:**
``` yql
SELECT FormatType(OptionalTypeHandle(
    TypeHandle(DataType("Bool"))
)); -- Bool?
```

## PgTypeName

**Сигнатура**
```
PgTypeName(PgTypeHandle)->String
```
Получение имени PostgreSQL типа из хендла типа, переданного в аргумент. Обратная функция - [PgTypeHandle](#pgtypehandle).

**Примеры:**
``` yql
SELECT PgTypeName(ParseTypeHandle("pgint4")); -- int4
```

## PgTypeHandle

**Сигнатура**
```
PgTypeHandle(String)->хендл типа
```
Построение хендла типа по имени PostgreSQL типа, переданного в аргумент. Обратная функция - [PgTypeName](#pgtypename).

**Примеры:**
``` yql
SELECT FormatType(PgTypeHandle("int4")); -- pgint4
```

## ListTypeHandle и StreamTypeHandle {#list-stream-typehandle}

**Сигнатура**
```
ListTypeHandle(TypeHandle)->хэндл списочного типа
StreamTypeHandle(TypeHandle)->хэндл потокового типа
```
Строит хендл типа списка или потока по переданному хендлу типа элемента.

**Примеры:**
``` yql
SELECT FormatType(ListTypeHandle(
    TypeHandle(DataType("Bool"))
)); -- List<Bool>
```

## EmptyListTypeHandle и EmptyDictTypeHandle

**Сигнатура**
```
EmptyListTypeHandle()->хэндл типа пустого списка
EmptyDictTypeHandle()->хэндл типа пустого словаря
```
Строит хендл типа пустого списка или словаря.

**Примеры:**
``` yql
SELECT FormatType(EmptyListTypeHandle()); -- EmptyList
```

## TupleTypeComponents

**Сигнатура**
```
TupleTypeComponents(TupleTypeHandle)->List<TypeHandle>
```
Получение списка хендлов типов элементов из хендла типа кортежа, переданного в аргумент. Обратная функция - [TupleTypeHandle](#tupletypehandle).

**Примеры:**
``` yql
SELECT ListMap(
   TupleTypeComponents(
       ParseTypeHandle("Tuple<Int32, String>")
   ),
   ($x)->{
       return FormatType($x)
   }
); -- ["Int32", "String"]
```

## TupleTypeHandle

**Сигнатура**
```
TupleTypeHandle(List<TypeHandle>)->хэндл типа кортежа
```
Построение хендла типа кортежа из хендлов типов элементов, переданных списком в аргумент. Обратная функция - [TupleTypeComponents](#tupletypecomponents).

**Примеры:**
``` yql
SELECT FormatType(
    TupleTypeHandle(
        AsList(
            ParseTypeHandle("Int32"),
            ParseTypeHandle("String")
        )
    )
); -- Tuple<Int32,String>
```

## StructTypeComponents

**Сигнатура**
```
StructTypeComponents(StructTypeHandle)->List<Struct<Name:String, Type:TypeHandle>>
```
Получение списка хендлов типов элементов и их имен из хендла типа структуры, переданного в аргумент. Обратная функция - [StructTypeHandle](#structtypehandle).

**Примеры:**
``` yql
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

## StructTypeHandle

**Сигнатура**
```
StructTypeHandle(List<Struct<Name:String, Type:TypeHandle>>)->хэндл типа структуры
```
Построение хендла типа структуры из хендлов типов элементов и имен, переданных списком в аргумент. Обратная функция - [StructTypeComponents](#structtypecomponents).

**Примеры:**
``` yql
SELECT FormatType(
    StructTypeHandle(
        AsList(
            AsStruct(ParseTypeHandle("Int32") as Type,"a" as Name),
            AsStruct(ParseTypeHandle("String") as Type, "b" as Name)
        )
    )
); -- Struct<'a':Int32,'b':String>
```

## DictTypeComponents

**Сигнатура**
```
DictTypeComponents(DictTypeHandle)->Struct<Key:TypeHandle, Payload:TypeHandle>
```
Получение хендла типа-ключа и хендла типа-значения - из хендла типа словаря, переданного в аргумент. Обратная функция - [DictTypeHandle](#dicttypehandle).

**Примеры:**
``` yql
$d = DictTypeComponents(ParseTypeHandle("Dict<Int32,String>"));

SELECT
    FormatType($d.Key),     -- Int32
    FormatType($d.Payload); -- String
```

## DictTypeHandle

**Сигнатура**
```
DictTypeHandle(TypeHandle, TypeHandle)->хэндл типа словаря
```
Построение хендла типа словаря из хендла типа-ключа и хендла типа-значения, переданных в аргументы. Обратная функция - [DictTypeComponents](#dicttypecomponents).

**Примеры:**
``` yql
SELECT FormatType(
    DictTypeHandle(
        ParseTypeHandle("Int32"),
        ParseTypeHandle("String")
    )
); -- Dict<Int32, String>
```

## ResourceTypeTag

**Сигнатура**
```
ResourceTypeTag(ResourceTypeHandle)->String
```
Получение тега из хендла типа ресурса, переданного в аргумент. Обратная функция - [ResourceTypeHandle](#resourcetypehandle).

**Примеры:**
``` yql
SELECT ResourceTypeTag(ParseTypeHandle("Resource<foo>")); -- foo
```

## ResourceTypeHandle

**Сигнатура**
```
ResourceTypeHandle(String)->хэндл типа ресурса
```
Построение хендла типа ресурса по значению тега, переданного в аргумент. Обратная функция - [ResourceTypeTag](#resourcetypetag).

**Примеры:**
``` yql
SELECT FormatType(ResourceTypeHandle("foo")); -- Resource<'foo'>
```

## TaggedTypeComponents

**Сигнатура**
```
TaggedTypeComponents(TaggedTypeHandle)->Struct<Base:TypeHandle, Tag:String>
```
Получение тега и базового типа из хендла декорированного типа, переданного в аргумент. Обратная функция - [TaggedTypeHandle](#taggedtypehandle).

**Примеры:**
``` yql
$t = TaggedTypeComponents(ParseTypeHandle("Tagged<Int32,foo>"));

SELECT FormatType($t.Base), $t.Tag; -- Int32, foo
```

## TaggedTypeHandle

**Сигнатура**
```
TaggedTypeHandle(TypeHandle, String)->хэндл декорированного типа
```
Построение хендла декорированного типа по хендлу базового типа и имени тега, переданных в аргументах. Обратная функция - [TaggedTypeComponents](#taggedtypecomponents).

**Примеры:**
``` yql
SELECT FormatType(TaggedTypeHandle(
    ParseTypeHandle("Int32"), "foo"
)); -- Tagged<Int32, 'foo'>
```

## VariantTypeHandle

**Сигнатура**
```
VariantTypeHandle(StructTypeHandle)->хэндл типа варианта над структурой
VariantTypeHandle(TupleTypeHandle)->хэндл типа варианта над кортежем
```
Построение хендла типа варианта по хендлу низлежащего типа, переданного в аргумент. Обратная функция - [VariantUnderlyingType](#variantunderlyingtype).

**Примеры:**
``` yql
SELECT FormatType(VariantTypeHandle(
    ParseTypeHandle("Tuple<Int32, String>")
)); -- Variant<Int32, String>
```

## VoidTypeHandle и NullTypeHandle

**Сигнатура**
```
VoidTypeHandle()->хэндл типа Void
NullTypeHandle()->хэндл типа Null
```
Построение хендла типов Void и Null соответственно.

**Примеры:**
``` yql
SELECT FormatType(VoidTypeHandle()); -- Void
SELECT FormatType(NullTypeHandle()); -- Null
```

## CallableTypeComponents

**Сигнатура**
```
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

**Примеры:**
``` yql
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

## CallableArgument

**Сигнатура**
```
CallableArgument(TypeHandle, [String, [List<String>]])->Struct<Flags:List<String>,Name:String,Type:TypeHandle>
```
Упаковка в структуру описания аргумента вызываемого значения для передачи в функцию [CallableTypeHandle](#callabletypehandle) по следующим аргументам:

1. Хендл типа аргумента.
2. Необязательное имя аргумента. Значение по умолчанию - пустая строка.
3. Необязательные флаги аргумента в виде списка строк. Значение по умолчанию - пустой список. Поддерживаемые флаги - "AutoMap".

## CallableTypeHandle

**Сигнатура**
```
CallableTypeHandle(TypeHandle, List<Struct<Flags:List<String>,Name:String,Type:TypeHandle>>, [Uint32, [String]])->хэндл типа вызываемого значения
```
Построение хендла типа вызываемого значения по следующим аргументам:

1. Хендл типа возвращаемого значения.
2. Список описаний аргументов, полученных через функцию [CallableArgument](#callableargument).
3. Необязательное количество необязательных аргументов в вызываемом значении. Значение по умолчанию - 0.
4. Необязательная метка для типа вызываемого значения. Значение по умолчанию - пустая строка.

Обратная функция - [CallableTypeComponents](#callabletypecomponents).

**Примеры:**
``` yql
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

## LambdaArgumentsCount

**Сигнатура**
```
LambdaArgumentsCount(LambdaFunction)->Uint32
```
Получение количества аргументов в лямбда-функции.

**Примеры:**
``` yql
SELECT LambdaArgumentsCount(($x, $y)->($x+$y))
; -- 2
```

## LambdaOptionalArgumentsCount

**Сигнатура**
```
LambdaOptionalArgumentsCount(LambdaFunction)->Uint32
```
Получение количества опциональных аргументов в лямбда-функции.

**Примеры:**
``` yql
SELECT LambdaOptionalArgumentsCount(($x, $y, $z?)->(if($x,$y,$z)))
; -- 1
```

{% endif %}
