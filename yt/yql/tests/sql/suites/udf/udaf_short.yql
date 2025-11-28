/* postgres can not */
/* syntax version 1 */
$script = @@
def create(item):
    return item

def add(state, item):
    return state + item 

def merge(state_a, state_b):
    return state_a + state_b 
@@;

$create = Python3::create(Callable<(Int64)->Int64>, $script);
$add = Python3::add(Callable<(Int64,Int64)->Int64>, $script);
$merge = Python3::merge(Callable<(Int64,Int64)->Int64>, $script);

SELECT UDAF(
    item,
    $create,
    $add,
    $merge
) FROM (
    SELECT
        CAST(LENGTH(value) AS Int64) AS item
    FROM plato.Input
); 
