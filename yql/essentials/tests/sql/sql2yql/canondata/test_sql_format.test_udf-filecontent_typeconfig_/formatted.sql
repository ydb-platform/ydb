/* postgres can not */
/*
syntax='proto3';

message Test {
  message InnerInner {
    string a = 1;
  }
  message Inner {
    InnerInner i = 1;
  }
  Inner inner = 1;
  Test test = 2;
}
*/
$dat = '{"inner":{"i":{"a":"hello"}},"test":null}';

$udfPar = Udf(Protobuf::Parse, FileContent('meta') AS TypeConfig);
$udfSer = Udf(Protobuf::Serialize, FileContent('meta') AS TypeConfig);

SELECT
    $udfPar($dat),
    Ensure('Success', $udfPar($dat) == $udfPar($udfSer($udfPar($dat))), 'Fail')
;
