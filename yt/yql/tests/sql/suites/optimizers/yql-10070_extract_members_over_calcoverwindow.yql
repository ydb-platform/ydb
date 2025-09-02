/* postgres can not */
USE plato;

INSERT INTO @source
SELECT Date('2019-03-04') AS `Date`,
    1l AS `Permalink`,
    1l AS `ClusterPermalink`,
    False AS `IsHead`,
    False AS `WasHead`,
    23 as dummy1;
COMMIT;

SELECT
    IF(
        NOT `WasHead` AND NOT `IsHead`,
        aggregate_list(AsStruct(`Permalink` AS `Permalink`, `Date` AS `Date`, `ClusterPermalink` AS ClusterPermalink)) OVER `w`
    ) AS `Occurence`
FROM 
    @source
WINDOW `w` AS (
    PARTITION BY `Permalink`
    ORDER BY `Date`
)
