/* syntax version 1 */
/* postgres can not */

PRAGMA DisableAnsiInForEmptyOrNullableItemsCollections;

USE plato;

-- Input: optkeys are 1-10,null
-- Dict: keys are 1,5,7,9,11,null

-- Either side can be: non-optional (P), optional without nulls (O), optional with nulls (N) - we test each of possible combination

$lp = (SELECT Unwrap(optkey) AS optkey FROM Input WHERE optkey IS NOT NULL);
$lo = (SELECT optkey FROM Input WHERE optkey IS NOT NULL);
$ln = (SELECT optkey FROM Input);

$rp = (SELECT Unwrap(key) AS key FROM Dict WHERE key IS NOT NULL);
$ro = (SELECT key FROM Dict WHERE key IS NOT NULL);
$rn = (SELECT key FROM Dict);


-- Right is P
SELECT optkey FROM $lp WHERE optkey NOT IN $rp ORDER BY optkey; -- [2,3,4,6,8,10]
SELECT optkey FROM $lo WHERE optkey NOT IN $rp ORDER BY optkey; -- [2,3,4,6,8,10]
SELECT optkey FROM $ln WHERE optkey NOT IN $rp ORDER BY optkey; -- [null,2,3,4,6,8,10]

-- Right is O
SELECT optkey FROM $lp WHERE optkey NOT IN $ro ORDER BY optkey; -- [2,3,4,6,8,10]
SELECT optkey FROM $lo WHERE optkey NOT IN $ro ORDER BY optkey; -- [2,3,4,6,8,10]
SELECT optkey FROM $ln WHERE optkey NOT IN $ro ORDER BY optkey; -- [null,2,3,4,6,8,10]

-- Right is N
SELECT optkey FROM $lp WHERE optkey NOT IN $rn ORDER BY optkey; -- [2,3,4,6,8,10]
SELECT optkey FROM $lo WHERE optkey NOT IN $rn ORDER BY optkey; -- [2,3,4,6,8,10]
SELECT optkey FROM $ln WHERE optkey NOT IN $rn ORDER BY optkey; -- [null,2,3,4,6,8,10]


-- 2, 4, 6, null
$extraDict = (
    SELECT 2 AS key UNION ALL
    SELECT 4 AS key UNION ALL
    SELECT 6 AS key UNION ALL
    SELECT NULL AS key
);

SELECT optkey FROM $lo WHERE optkey != 10 AND optkey NOT IN $ro AND optkey IN $extraDict AND optkey != 4 ORDER BY optkey; -- [2,6]
SELECT optkey FROM $ln WHERE optkey != 10 AND optkey NOT IN $rn AND optkey IN $extraDict AND optkey != 4 ORDER BY optkey; -- [2,6]


-- Empty dict
SELECT optkey FROM $ln WHERE optkey NOT IN (SELECT * FROM $rn LIMIT 0) ORDER BY optkey; -- [1-10,null]

