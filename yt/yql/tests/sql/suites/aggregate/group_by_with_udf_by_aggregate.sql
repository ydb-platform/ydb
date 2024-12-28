/* syntax version 1 */
/* postgres can not */
$majority_vote = Python::majority_vote(
    Callable<(List<String?>)->String>,
    @@
def majority_vote(values):
    counters = {}
    for value in values:
        counters[value] = counters.get(value, 0) + 1
    return sorted((count, value) for value, count in counters.items())[-1][1]
    @@
);

select count(*), val, $majority_vote(aggregate_list(subkey)) from plato.Input group by cast(key as uint32) % 2 as val;
