$t = [
    AsStruct('a' AS `key`, 1 AS val),
    AsStruct('a' AS `key`, 2 AS val),
    AsStruct('b' AS `key`, 1 AS val)
];

SELECT k, SUM(val)
FROM AS_TABLE($t)
WHERE `key` == 'a'
GROUP BY `key` AS k

