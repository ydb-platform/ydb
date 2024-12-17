/* syntax version 1 */
$lists = AsList(AsList("one", "two", "three"), AsList("head", null), AsList(null, "tail"), ListCreate(String?));
$map = ($l) -> { RETURN AsStruct(ListHead($l) AS head,  ListLast($l) AS tail) };
$structs = ListMap($lists, $map);
SELECT YQL::FilterNullMembers($structs), YQL::SkipNullMembers($structs);
