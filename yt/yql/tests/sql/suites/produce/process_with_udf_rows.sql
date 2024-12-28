/* postgres can not */
$udf = Python::process(
Callable<
    ()->Stream<Struct<result:Int64>>
>, @@
def process():
    for row in range(10):
        result = row
        yield locals()
@@);

$users = (
    SELECT `key` AS age, `value` AS name FROM plato.Input0
);

PROCESS $users
USING $udf();
