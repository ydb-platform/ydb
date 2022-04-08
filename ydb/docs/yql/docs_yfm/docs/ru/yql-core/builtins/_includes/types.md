# Функции для работы с типами данных

## FormatType {#formattype}

Сериализация типа {% if feature_codegen %} или хендла типа{% endif %} в человекочитаемую строку. Это полезно для отладки, а также будет использоваться в последующих примерах данного раздела. [Документация по формату](../../types/type_string.md).

## ParseType {#parsetype}

Построение типа по строке с его описанием. [Документация по её формату](../../types/type_string.md).

**Примеры**
``` yql
SELECT FormatType(ParseType("List<Int32>"));  -- List<int32>
```

## TypeOf {#typeof}

Получение типа значения, переданного в аргумент.

**Примеры**
``` yql
SELECT FormatType(TypeOf("foo"));  -- String
```
``` yql
SELECT FormatType(TypeOf(AsTuple(1, 1u))); -- Tuple<Int32,Uint32>
```

## InstanceOf {#instanceof}

Возвращает экземпляр указанного типа, который можно использовать только для получения типа результата выражения с его использованием.

Если этот экземпляр останется в графе вычислений к моменту окончания оптимизации, то операция будет завершена с ошибкой.

**Примеры**
``` yql
SELECT FormatType(TypeOf(
    InstanceOf(ParseType("Int32")) +
    InstanceOf(ParseType("Double"))
)); -- Double, так как "Int32 + Double" возвращает Double
```

## DataType {#datatype}

Возвращает тип для [примитивных типов данных](../../types/primitive.md) по его имени.

**Примеры**
``` yql
SELECT FormatType(DataType("Bool")); -- Bool
SELECT FormatType(DataType("Decimal","5","1")); -- Decimal(5,1)
```

## OptionalType {#optionaltype}

Добавляет в переданный тип возможность содержать `NULL`.

**Примеры**
``` yql
SELECT FormatType(OptionalType(DataType("Bool"))); -- Bool?
```

## ListType и StreamType {#listtype}

Строит тип списка или потока по переданному типу элемента.

**Примеры**
``` yql
SELECT FormatType(ListType(DataType("Bool"))); -- List<Bool>
```

## DictType {#dicttype}

Строит тип словаря по переданным типам ключа (первый аргумент) и значения (второй аргумент).

**Примеры**
``` yql
SELECT FormatType(DictType(
    DataType("String"),
    DataType("Double")
)); -- Dict<String,Double>
```

## TupleType {#tupletype}

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

Строит тип структуры по переданным типам элементов. Для указания имен элементов используется стандартный синтаксис именованных аргументов.

**Примеры**
``` yql
SELECT FormatType(StructType(
    DataType("Bool") AS MyBool,
    ListType(DataType("String")) AS StringList
)); -- Struct<'MyBool':Bool,'StringList':List<String>>
```

## VariantType {#varianttype}

Возвращает тип варианта по низлежащему типу (структуры или кортежа).

**Примеры**
``` yql
SELECT FormatType(VariantType(
  ParseType("Struct<foo:Int32,bar:Double>")
)); -- Variant<'bar':Double,'foo':Int32>
```

## ResourceType {#resourcetype}

Возвращает тип [ресурса](../../types/special.md) по переданной строковой метке.

**Примеры**
``` yql
SELECT FormatType(ResourceType("Foo")); -- Resource<'Foo'>
```

## CallableType {#callabletype}

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

Возвращают одноименные [специальные типы данных](../../types/special.md). Аргументов нет, так как они не параметризуются.

**Примеры**
``` yql
SELECT FormatType(VoidType()); -- Void
```

## OptionalItemType, ListItemType и StreamItemType {#optionalitemtype}

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

Возвращают тип ключа или значения по типу словаря.

**Примеры**
``` yql
SELECT FormatType(DictKeyType(
  ParseType("Dict<Int32,String>")
)); -- Int32
```

## TupleElementType {#tupleelementtype}

Возвращает тип элемента кортежа по типу кортежа и индексу элемента (индекс с нуля).

**Примеры**
``` yql
SELECT FormatType(TupleElementType(
  ParseType("Tuple<Int32,Double>"), "1"
)); -- Double
```

## StructMemberType {#structmembertype}

Возвращает тип элемента структуры по типу структуры и имени элемента.

**Примеры**
``` yql
SELECT FormatType(StructMemberType(
  ParseType("Struct<foo:Int32,bar:Double>"), "foo"
)); -- Int32
```

## CallableResultType и CallableArgumentType {#callableresulttype}

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

**Примеры:**
``` yql
SELECT FormatType(TypeHandle(TypeOf("foo")));  -- String
```
## EvaluateType

Получение типа из хендла типа, переданного в аргумент. Функция вычисляется до начала основного расчета, как и [EvaluateExpr](../basic.md#evaluate_expr_atom).

**Примеры:**
``` yql
SELECT FormatType(EvaluateType(TypeHandle(TypeOf("foo"))));  -- String
```

## ParseTypeHandle

Построение хендла типа по строке с его описанием. [Документация по её формату](../../types/type_string.md).

**Примеры:**
``` yql
SELECT FormatType(ParseTypeHandle("List<Int32>"));  -- List<int32>
```

## TypeKind

Получение названия верхнего уровня типа из хендла типа, переданного в аргумент.

**Примеры:**
``` yql
SELECT TypeKind(TypeHandle(TypeOf("foo")));  -- Data
SELECT TypeKind(ParseTypeHandle("List<Int32>"));  -- List
```

## DataTypeComponents

Получение названия и параметров [примитивного типа данных](../../types/primitive.md) из хендла примитивного типа, переданного в аргумент. Обратная функция - [DataTypeHandle](#datatypehandle).

**Примеры:**
``` yql
SELECT DataTypeComponents(TypeHandle(TypeOf("foo")));  -- ["String"]
SELECT DataTypeComponents(ParseTypeHandle("Decimal(4,1)"));  -- ["Decimal", "4", "1"]
```

## DataTypeHandle

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

Добавляет в переданный хендл типа возможность содержать `NULL`.

**Примеры:**
``` yql
SELECT FormatType(OptionalTypeHandle(
    TypeHandle(DataType("Bool"))
)); -- Bool?
```

## ListTypeHandle и StreamTypeHandle {#list-stream-typehandle}

Строит хендл типа списка или потока по переданному хендлу типа элемента.

**Примеры:**
``` yql
SELECT FormatType(ListTypeHandle(
    TypeHandle(DataType("Bool"))
)); -- List<Bool>
```

## EmptyListTypeHandle и EmptyDictTypeHandle

Строит хендл типа пустого списка или словаря.

**Примеры:**
``` yql
SELECT FormatType(EmptyListTypeHandle()); -- EmptyList
```

## TupleTypeComponents

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

Получение хендла типа-ключа и хендла типа-значения - из хендла типа словаря, переданного в аргумент. Обратная функция - [DictTypeHandle](#dicttypehandle).

**Примеры:**
``` yql
$d = DictTypeComponents(ParseTypeHandle("Dict<Int32,String>"));

SELECT
    FormatType($d.Key),     -- Int32
    FormatType($d.Payload); -- String
```

## DictTypeHandle

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

Получение тега из хендла типа ресурса, переданного в аргумент. Обратная функция - [ResourceTypeHandle](#resourcetypehandle).

**Примеры:**
``` yql
SELECT ResourceTypeTag(ParseTypeHandle("Resource<foo>")); -- foo
```

## ResourceTypeHandle

Построение хендла типа ресурса по значению тега, переданного в аргумент. Обратная функция - [ResourceTypeTag](#resourcetypetag).

**Примеры:**
``` yql
SELECT FormatType(ResourceTypeHandle("foo")); -- Resource<'foo'>
```

## TaggedTypeComponents

Получение тега и базового типа из хендла декорированного типа, переданного в аргумент. Обратная функция - [TaggedTypeHandle](#taggedtypehandle).

**Примеры:**
``` yql
$t = TaggedTypeComponents(ParseTypeHandle("Tagged<Int32,foo>"));

SELECT FormatType($t.Base), $t.Tag; -- Int32, foo
```

## TaggedTypeHandle

Построение хендла декорированного типа по хендлу базового типа и имени тега, переданных в аргументах. Обратная функция - [TaggedTypeComponents](#taggedtypecomponents).

**Примеры:**
``` yql
SELECT FormatType(TaggedTypeHandle(
    ParseTypeHandle("Int32"), "foo"
)); -- Tagged<Int32, 'foo'>
```

## VariantTypeHandle

Построение хендла типа варианта по хендлу низлежащего типа, переданного в аргумент. Обратная функция - [VariantUnderlyingType](#variantunderlyingtype).

**Примеры:**
``` yql
SELECT FormatType(VariantTypeHandle(
    ParseTypeHandle("Tuple<Int32, String>")
)); -- Variant<Int32, String>
```

## VoidTypeHandle и NullTypeHandle

Построение хендла типов Void и Null соответственно.

**Примеры:**
``` yql
SELECT FormatType(VoidTypeHandle()); -- Void
SELECT FormatType(NullTypeHandle()); -- Null
```

## CallableTypeComponents

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

Упаковка в структуру описания аргумента вызываемого значения для передачи в функцию [CallableTypeHandle](#callabletypehandle) по следующим аргументам:

1. Хендл типа аргумента.
2. Необязательное имя аргумента. Значение по умолчанию - пустая строка.
3. Необязательные флаги аргумента в виде списка строк. Значение по умолчанию - пустой список. Поддерживаемые флаги - "AutoMap".

## CallableTypeHandle

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

Получение количества аргументов в лямбда-функции.

**Примеры:**
``` yql
SELECT LambdaArgumentsCount(($x, $y)->($x+$y))
; -- 2
```

{% endif %}
