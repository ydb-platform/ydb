/* postgres can not */
/* kikimr can not */
/* syntax version 1 */
$script = @@
def save(item):
    return item

def load(item):
    return item
@@;

$save = Python3::save(Callable<(String)->Resource<Python3>>, $script);
$load = Python3::load(Callable<(Resource<Python3>)->String>, $script);

$input = (
    SELECT key, AsList($save(value), $save(subkey)) AS resourceList FROM plato.Input
);

SELECT key, $load(resourceList) AS value FROM $input FLATTEN BY resourceList;
