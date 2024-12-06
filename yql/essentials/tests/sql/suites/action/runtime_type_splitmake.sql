/* syntax version 1 */
/* postgres can not */
$formatTagged = ($x)->{return AsStruct(FormatType($x.Base) as Base, $x.Tag as Tag)};
$formatArgument = ($x)->{return AsStruct(FormatType($x.Type) as Type, $x.Name as Name, $x.Flags as Flags)};
$formatCallable = ($x)->{return AsStruct(
    $x.OptionalArgumentsCount as OptionalArgumentsCount,
    $x.Payload as Payload,
    FormatType($x.Result) as Result,
    ListMap($x.Arguments, $formatArgument) as Arguments
)};

select
    DataTypeComponents(ParseTypeHandle("Int32")),
    DataTypeComponents(ParseTypeHandle("Decimal(4,1)")),
    FormatType(DataTypeHandle(AsList("Int32"))),
    FormatType(DataTypeHandle(AsList("Decimal","4","1"))),
    FormatType(OptionalItemType(ParseTypeHandle("Int32?"))),
    FormatType(OptionalTypeHandle(ParseTypeHandle("Int32"))),
    FormatType(ListItemType(ParseTypeHandle("List<Int32>"))),
    FormatType(ListTypeHandle(ParseTypeHandle("Int32"))),
    FormatType(StreamItemType(ParseTypeHandle("Stream<Int32>"))),
    FormatType(StreamTypeHandle(ParseTypeHandle("Int32"))),
    ListMap(
        TupleTypeComponents(ParseTypeHandle("Tuple<Int32,String>")),
        ($x)->{return FormatType($x)}),
    FormatType(TupleTypeHandle(ListMap(
        AsList("Int32","String"), ($x)->{return ParseTypeHandle($x)}))),
    ListMap(
        StructTypeComponents(ParseTypeHandle("Struct<foo:Int32,bar:String>")),
        ($x)->{return AsTuple($x.Name, FormatType($x.Type))}),
    FormatType(StructTypeHandle(ListMap(
        AsList(AsTuple("foo", "Int32"),AsTuple("bar", "String")), 
        ($x)->{return AsStruct($x.0 as Name,ParseTypeHandle($x.1) as Type)}))),
    StaticMap(DictTypeComponents(ParseTypeHandle("Dict<String,Int32>")),
        ($x)->{return FormatType($x)}),
    FormatType(DictTypeHandle(ParseTypeHandle("String"),ParseTypeHandle("Int32"))),
    ResourceTypeTag(ParseTypeHandle("Resource<foo>")),
    FormatType(ResourceTypeHandle("foo")),
    $formatTagged(TaggedTypeComponents(ParseTypeHandle("Tagged<String,foo>"))),
    FormatType(TaggedTypeHandle(ParseTypeHandle("String"),"foo")),
    FormatType(VariantUnderlyingType(ParseTypeHandle("Variant<Int32,String>"))),
    FormatType(VariantTypeHandle(ParseTypeHandle("Tuple<Int32,String>"))),
    FormatType(VariantUnderlyingType(ParseTypeHandle("Variant<a:Int32,b:String>"))),
    FormatType(VariantTypeHandle(ParseTypeHandle("Struct<a:Int32,b:String>"))),
    FormatType(VoidTypeHandle()),
    FormatType(NullTypeHandle()),
    $formatCallable(CallableTypeComponents(ParseTypeHandle("(Int32,[bar:Double?{Flags:AutoMap}])->String{Payload:foo}"))),
    FormatType(CallableTypeHandle(ParseTypeHandle("String"),AsList(
        CallableArgument(ParseTypeHandle("Int32")),
        CallableArgument(ParseTypeHandle("Double?"), "bar", AsList("AutoMap"))))),
    FormatType(CallableTypeHandle(ParseTypeHandle("String"),AsList(
        CallableArgument(ParseTypeHandle("Int32")),
        CallableArgument(ParseTypeHandle("Double?"), "bar", AsList("AutoMap"))), 
        1, "foo"));
