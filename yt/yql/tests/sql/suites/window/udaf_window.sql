/* postgres can not */
/* syntax version 1 */
$script = @@
import heapq
import json

N_SMALLEST = 3

def create(item):
    return [item]

def add(state, item):
    heapq.heappush(state, item)
    return heapq.nsmallest(N_SMALLEST, state)

def merge(state_a, state_b):
    merged = heapq.merge(state_a, state_b)
    return heapq.nsmallest(N_SMALLEST, merged)

def get_result(state):
    result = heapq.nsmallest(N_SMALLEST, state)
    return '%d smallest items: %s' % (
        N_SMALLEST,
        ', '.join(map(str, result))
    )

def serialize(state):
    return json.dumps(state)

def deserialize(serialized):
    return json.loads(serialized)
@@;

$create = Python3::create(Callable<(Double)->Resource<Python3>>, $script);
$add = Python3::add(Callable<(Resource<Python3>,Double)->Resource<Python3>>, $script);
$merge = Python3::merge(Callable<(Resource<Python3>,Resource<Python3>)->Resource<Python3>>, $script);
$get_result = Python3::get_result(Callable<(Resource<Python3>)->String>, $script);
$serialize = Python3::serialize(Callable<(Resource<Python3>)->String>, $script);
$deserialize = Python3::deserialize(Callable<(String)->Resource<Python3>>, $script);

SELECT UDAF(
    CAST(key AS Double),
    $create,
    $add,
    $merge,
    $get_result,
    $serialize,
    $deserialize
) OVER w
FROM plato.Input
WINDOW w AS (ORDER by value);
