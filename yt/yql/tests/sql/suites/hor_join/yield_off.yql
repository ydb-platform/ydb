/* syntax version 1 */
/* postgres can not */
USE plato;
pragma config.flags("UdfSupportsYield","false");

$s = @@
import yql

def f(input, a):
    for x in input:
        yield x
@@;

$f = Python::f(Callable<(Stream<Struct<key:String,subkey:String,value:String>>,Int32)->Stream<Struct<key:String,subkey:String,value:String>>>, $s);


SELECT * FROM (
    PROCESS Input using $f(TableRows(), 1)
    UNION ALL
    PROCESS Input using $f(TableRows(), 2)
) AS x
ORDER BY key, subkey, value
;
