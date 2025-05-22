/* postgres can not */
/* kikimr can not */
/* syntax version 1 */
$ad = AsStruct(1 AS a, 4 AS d);
$b = AsStruct(2 AS b);
$c_name = 'c';
SELECT
    TryMember($ad, $c_name, "foo") AS c,
    TryMember($ad, "d", NULL) AS d,
    AddMember($ad, $c_name, 3) AS acd,
    ReplaceMember($ad, "a", 5) AS a5,
    RemoveMember($ad, 'd') AS a,
    ForceRemoveMember($ad, $c_name) AS ad,
    ExpandStruct($b, 1 AS a) AS ab,
    CombineMembers($ad, $b) AS abd,
    FlattenMembers(AsTuple("fo" || "o", $ad), AsTuple("bar", $b)) AS foobar;
