--sanitizer ignore memory
$udfScript = @@
import yql
def mapper(records):
    yql.g = records
    for record in records:
        yield dict(yid=b"bla", rnd=0.)
@@;

$udf = Python3::mapper(Callable<(Stream<Struct<key:String, subkey:String, value:String>>)->Stream<Struct<yid:String, rnd:Double>>>, $udfScript);

$rows = [<|key: "1", subkey: "2", value: "3"|>];

PROCESS AS_TABLE($rows) using $udf(TableRows());
