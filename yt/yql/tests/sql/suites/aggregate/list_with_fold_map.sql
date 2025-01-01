/* syntax version 1 */
USE plato;

$data = (
    SELECT
        YQL::FoldMap(
            counters,
            names,
            ($counter, $names) -> {
                RETURN AsTuple(Unwrap($names[$counter]), $names);
            }
        ) AS profile,
        id
    FROM Input
);

SELECT
    AGGREGATE_LIST(profile) AS profiles,
    id
FROM $data
GROUP BY id; 
