$node1 = Yson::Parse(Yson(@@{a=123}@@));
$node2 = Yson::Parse(Yson(@@{a=#}@@));
$node3 = Yson::Parse(Yson(@@{a=123;b=#}@@));
$node4 = Yson::Parse(Yson(@@[123;#]@@));
$node5 = Yson::Parse(Yson(@@{a=1;b=#;c=1;d=#;e=#}@@));
$node6 = Yson::Parse(Yson(@@{b=1;a=<c=1;d=#;e=3>23}@@));
$node7 = Yson::Parse(Yson(@@{b=1;a=<c=1;d=#;e=3>#}@@));
$node8 = Yson::Parse(Yson(@@<d=#>23@@));

SELECT
    Yson::SerializeJson($node1, true as SkipMapEntity) AS res1,
    Yson::SerializeJson($node2, true as SkipMapEntity) AS res2,
    Yson::SerializeJson($node3, true as SkipMapEntity) AS res3,
    Yson::SerializeJson($node4, true as SkipMapEntity) AS res4,
    Yson::SerializeJson($node5, true as SkipMapEntity) AS res5,
    Yson::SerializeJson($node6, true as SkipMapEntity) AS res6,
    Yson::SerializeJson($node7, true as SkipMapEntity) AS res7,
    Yson::SerializeJson($node8, true as SkipMapEntity) AS res8;
