/* postgres can not */
/* syntax version 1 */
$dict = AsDict(AsTuple("foo", 3), AsTuple("bar", 4));

SELECT
    DictKeys($dict),
    DictPayloads($dict),
    DictItems($dict),
    DictLookup($dict, "foo"),
    DictLookup($dict, "baz"),
    DictContains($dict, "foo"),
    DictContains($dict, "baz"),
    DictCreate(String, Tuple<String,Double?>),
    DictCreate(Tuple<Int32?,String>, OptionalType(DataType("String")));
    
