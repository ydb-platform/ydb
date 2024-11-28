/* syntax version 1 */
/* postgres can not */
USE plato;

$data = (
    SELECT
        CAST(key AS uint32) AS age,
        CAST(subkey AS uint32) AS region,
        value AS name
    FROM Input
);

--insert into Output
SELECT
    region,
    max(
        CASE
            WHEN age % 10u BETWEEN 1u AND region % 10u
                THEN age
            ELSE 0u
        END
    ) AS max_age_at_range_intersect
FROM $data
GROUP BY
    region
ORDER BY
    region;
