/* syntax version 1 */
/* postgres can not */
$formatTagged = ($x) -> {
    RETURN AsStruct(FormatType($x.Base) AS Base, $x.Tag AS Tag);
};

$formatArgument = ($x) -> {
    RETURN AsStruct(FormatType($x.Type) AS Type, $x.Name AS Name, $x.Flags AS Flags);
};

$formatCallable = ($x) -> {
    RETURN AsStruct(
        $x.OptionalArgumentsCount AS OptionalArgumentsCount,
        $x.Payload AS Payload,
        FormatType($x.Result) AS Result,
        ListMap($x.Arguments, $formatArgument) AS Arguments
    );
};

SELECT
    DataTypeComponents(ParseTypeHandle('Int32')),
    DataTypeComponents(ParseTypeHandle('Decimal(4,1)')),
    FormatType(DataTypeHandle(AsList('Int32'))),
    FormatType(DataTypeHandle(AsList('Decimal', '4', '1'))),
    FormatType(OptionalItemType(ParseTypeHandle('Int32?'))),
    FormatType(OptionalTypeHandle(ParseTypeHandle('Int32'))),
    FormatType(ListItemType(ParseTypeHandle('List<Int32>'))),
    FormatType(ListTypeHandle(ParseTypeHandle('Int32'))),
    FormatType(StreamItemType(ParseTypeHandle('Stream<Int32>'))),
    FormatType(StreamTypeHandle(ParseTypeHandle('Int32'))),
    ListMap(
        TupleTypeComponents(ParseTypeHandle('Tuple<Int32,String>')),
        ($x) -> {
            RETURN FormatType($x);
        }
    ),
    FormatType(
        TupleTypeHandle(
            ListMap(
                AsList('Int32', 'String'), ($x) -> {
                    RETURN ParseTypeHandle($x);
                }
            )
        )
    ),
    ListMap(
        StructTypeComponents(ParseTypeHandle('Struct<foo:Int32,bar:String>')),
        ($x) -> {
            RETURN AsTuple($x.Name, FormatType($x.Type));
        }
    ),
    FormatType(
        StructTypeHandle(
            ListMap(
                AsList(AsTuple('foo', 'Int32'), AsTuple('bar', 'String')),
                ($x) -> {
                    RETURN AsStruct($x.0 AS Name, ParseTypeHandle($x.1) AS Type);
                }
            )
        )
    ),
    StaticMap(
        DictTypeComponents(ParseTypeHandle('Dict<String,Int32>')),
        ($x) -> {
            RETURN FormatType($x);
        }
    ),
    FormatType(DictTypeHandle(ParseTypeHandle('String'), ParseTypeHandle('Int32'))),
    ResourceTypeTag(ParseTypeHandle('Resource<foo>')),
    FormatType(ResourceTypeHandle('foo')),
    $formatTagged(TaggedTypeComponents(ParseTypeHandle('Tagged<String,foo>'))),
    FormatType(TaggedTypeHandle(ParseTypeHandle('String'), 'foo')),
    FormatType(VariantUnderlyingType(ParseTypeHandle('Variant<Int32,String>'))),
    FormatType(VariantTypeHandle(ParseTypeHandle('Tuple<Int32,String>'))),
    FormatType(VariantUnderlyingType(ParseTypeHandle('Variant<a:Int32,b:String>'))),
    FormatType(VariantTypeHandle(ParseTypeHandle('Struct<a:Int32,b:String>'))),
    FormatType(VoidTypeHandle()),
    FormatType(NullTypeHandle()),
    $formatCallable(CallableTypeComponents(ParseTypeHandle('(Int32,[bar:Double?{Flags:AutoMap}])->String{Payload:foo}'))),
    FormatType(
        CallableTypeHandle(
            ParseTypeHandle('String'), AsList(
                CallableArgument(ParseTypeHandle('Int32')),
                CallableArgument(ParseTypeHandle('Double?'), 'bar', AsList('AutoMap'))
            )
        )
    ),
    FormatType(
        CallableTypeHandle(
            ParseTypeHandle('String'), AsList(
                CallableArgument(ParseTypeHandle('Int32')),
                CallableArgument(ParseTypeHandle('Double?'), 'bar', AsList('AutoMap'))
            ),
            1, 'foo'
        )
    )
;
