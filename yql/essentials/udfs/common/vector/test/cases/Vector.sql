/* syntax version 1 */

$typing = TupleType(VoidType(), VoidType(), String);

$vectorCreate = YQL::Udf(AsAtom("Vector.Create"), Void(), $typing);
$vectorEmplace = YQL::Udf(AsAtom("Vector.Emplace"), Void(), $typing);
$vectorSwap = YQL::Udf(AsAtom("Vector.Swap"), Void(), $typing);
$vectorGetResult = YQL::Udf(AsAtom("Vector.GetResult"), Void(), $typing);

$a = $vectorCreate(0);

$a = $vectorEmplace($a, 0, "test1");
$a = $vectorEmplace($a, 1, "test2");
$a = $vectorEmplace($a, 2, "test3");
$state1 = $vectorGetResult($a);

$a = $vectorEmplace($a, 1, "test22");
$state2 = $vectorGetResult($a);

$a = $vectorSwap($a, 0, 2);
$state3 = $vectorGetResult($a);

SELECT $state1, $state2, $state3;
