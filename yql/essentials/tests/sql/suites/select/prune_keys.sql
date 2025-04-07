/* postgres can not */
pragma warning("disable", "4510");

$id = ($x) -> { RETURN $x; };
SELECT Yql::PruneAdjacentKeys(AsList(1,1,1,2,3,3,4,5), $id);
SELECT Yql::PruneKeys(AsList(1,1,1,1,1,1,1), $id);

SELECT Yql::PruneAdjacentKeys([], $id);
SELECT Yql::PruneKeys([], $id);

$mod2 = ($x) -> { RETURN $x % 2; };
SELECT ListLength(Yql::PruneKeys(AsList(1,1,1,3,3,3,3), $mod2));

-- optimize tests

$get_a = ($x) -> { RETURN <|a:$x.a|>; };
select Yql::ExtractMembers(Yql::PruneKeys(AsList(<|a:1, b:2, c:3|>, <|a:1, b:3, c:4|>), $get_a), AsTuple(EvaluateAtom('a')));
select Yql::ExtractMembers(Yql::PruneAdjacentKeys(AsList(<|a:1, b:2, c:3|>, <|a:1, b:3, c:4|>), $get_a), AsTuple(EvaluateAtom('a')));


$get_a_b = ($x) -> { RETURN <|a:$x.a, b:$x.b|>; };
$prune_keys_result = Yql::PruneKeys(AsList(<|a:1, b:2, c:3|>, <|a:1, b:3, c:4|>), $get_a_b);
select Yql::ExtractMembers($prune_keys_result, AsTuple(EvaluateAtom('a'))), Yql::ExtractMembers($prune_keys_result, AsTuple(EvaluateAtom('b')));

$prune_adjacent_keys_result = Yql::PruneAdjacentKeys(AsList(<|a:1, b:2, c:3|>, <|a:1, b:3, c:4|>), $get_a_b);
select Yql::ExtractMembers($prune_adjacent_keys_result, AsTuple(EvaluateAtom('a'))), Yql::ExtractMembers($prune_adjacent_keys_result, AsTuple(EvaluateAtom('b')));


$get_a_bp1_list = ($x) -> { RETURN AsList(<|a:$x.a, b:$x.b+1|>); };
select Yql::PruneKeys(Yql::FlatMap(AsList(<|a:1, b:2, c:3|>, <|a:1, b:3, c:4|>), $get_a_bp1_list), $get_a);
select Yql::PruneKeys(Yql::FlatMap(AsList(<|a:1, b:2, c:3|>, <|a:1, b:3, c:4|>), $get_a_bp1_list), $get_a_b);
select Yql::PruneAdjacentKeys(Yql::FlatMap(AsList(<|a:1, b:2, c:3|>, <|a:1, b:3, c:4|>), $get_a_bp1_list), $get_a);
select Yql::PruneAdjacentKeys(Yql::FlatMap(AsList(<|a:1, b:2, c:3|>, <|a:1, b:3, c:4|>), $get_a_bp1_list), $get_a_b);


select Yql::PruneKeys(Yql::PruneKeys(AsList(<|a:1, b:2, c:3|>, <|a:1, b:3, c:4|>), $get_a), $get_a);
select Yql::PruneKeys(Yql::PruneKeys(AsList(<|a:1, b:2, c:3|>, <|a:1, b:3, c:4|>), $get_a_b), $get_a);
select Yql::PruneKeys(Yql::PruneAdjacentKeys(AsList(<|a:1, b:2, c:3|>, <|a:1, b:3, c:4|>), $get_a), $get_a);
select Yql::PruneAdjacentKeys(Yql::PruneAdjacentKeys(AsList(<|a:1, b:2, c:3|>, <|a:1, b:3, c:4|>), $get_a), $get_a);
