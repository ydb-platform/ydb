/* postgres can not */
/* syntax version 1 */
$s1 = AsStruct(1 AS k1, '2' AS k2, ['3', '4'] AS k3);
$s2 = AsStruct('10' AS k1, [20, 30] AS k2, 40 AS k3);
$s3 = AsStruct([100, 200] AS k1, 300 AS k2, '400' AS k3);
$t1 = AsTuple(1, '2', ['3', '4']);
$t2 = AsTuple('10', [20, 30], 40);
$t3 = AsTuple([100, 200], 300, '400');

SELECT
    StaticZip($s1, $s2, $s3) AS structs,
    StaticZip($t1, $t2, $t3) AS tuples,
    StaticZip(AsStruct(), AsStruct()) AS empty_structs,
    StaticZip(AsTuple(), AsTuple()) AS empty_tuples,
    StaticZip($s1) AS single_struct,
    StaticZip($t1) AS single_tuple,
;
