/* syntax version 1 */
$fn1 = YQL::Udf(AsAtom("Prefix.Apply"), Void(), Void(), AsAtom("pre1-"));
$fn2 = YQL::Udf(AsAtom("Prefix.Apply"), Void(), Void(), AsAtom("pre2-"));
$fn3 = YQL::Udf(AsAtom("Prefix.Apply"), Void(), Void(), AsAtom("pre3-"));

SELECT $fn1("x"), $fn2("x"), $fn3("x") AS out;
