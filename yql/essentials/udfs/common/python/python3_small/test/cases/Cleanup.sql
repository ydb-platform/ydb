--sanitizer ignore memory
$udfScript = @@
import yql
def mapper(records):
    yql.g = records
    for record in records:
        yield dict(yid=b"bla", rnd=0.)
@@;

$udf = Python3::mapper(Callable<(Stream<Struct<key:String, subkey:String, value:String>>)->Stream<Struct<yid:String, rnd:Double>>>, $udfScript);

PROCESS Input using $udf(TableRows());
