/* syntax version 1 */
/* postgres can not */
SELECT
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY('minby'))
FROM (
    SELECT
        TableRow() AS nums
    FROM
        AS_TABLE([<|x: (1, 6)|>, <|x: (3, 4)|>, <|x: (5, 2)|>])
);

SELECT
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY('maxby', 2))
FROM (
    SELECT
        TableRow() AS nums
    FROM
        AS_TABLE([<|x: (1, 6)|>, <|x: (3, 4)|>, <|x: (5, 2)|>])
);
