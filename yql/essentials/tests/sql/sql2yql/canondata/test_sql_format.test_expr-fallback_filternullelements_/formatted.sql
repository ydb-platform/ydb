/* syntax version 1 */
$lists = AsList(AsList('one', 'two', 'three'), AsList('head', NULL), AsList(NULL, 'tail'), ListCreate(String?));

$map = ($l) -> {
    RETURN AsTuple(ListHead($l), ListLast($l));
};

$structs = ListMap($lists, $map);

SELECT
    YQL::FilterNullElements($structs),
    YQL::SkipNullElements($structs)
;
