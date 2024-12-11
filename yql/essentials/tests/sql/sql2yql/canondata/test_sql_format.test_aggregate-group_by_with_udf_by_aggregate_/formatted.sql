/* syntax version 1 */
/* postgres can not */
$majority_vote = Python::majority_vote(
    Callable<(List<String?>) -> String>,
    @@
def majority_vote(values):
    counters = {}
    for value in values:
        counters[value] = counters.get(value, 0) + 1
    return sorted((count, value) for value, count in counters.items())[-1][1]
    @@
);

SELECT
    count(*),
    val,
    $majority_vote(aggregate_list(subkey))
FROM
    plato.Input
GROUP BY
    CAST(key AS uint32) % 2 AS val
;
