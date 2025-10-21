/* postgres can not */
/* syntax version 1 */

$s1 = AsStruct(1 as k1, "2" as k2, ["3", "4"] as k3);
$s2 = AsStruct("10" as k1, [20, 30] as k2, 40 as k3);
$s3 = AsStruct([100, 200] as k1, 300 as k2, "400" as k3);


$t1 = AsTuple(1, "2", ["3", "4"]);
$t2 = AsTuple("10", [20, 30], 40);
$t3 = AsTuple([100, 200], 300, "400");

SELECT
    StaticZip($s1, $s2, $s3) as structs,
    StaticZip($t1, $t2, $t3) as tuples,
    StaticZip(AsStruct(), AsStruct()) as empty_structs,
    StaticZip(AsTuple(), AsTuple()) as empty_tuples,
    StaticZip($s1) as single_struct,
    StaticZip($t1) as single_tuple,
;
