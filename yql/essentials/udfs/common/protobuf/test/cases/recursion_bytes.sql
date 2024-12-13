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

$config = @@{
  "name": "Test",
  "format": "json",
  "skip": 0,
  "lists": {
    "optional": false
  },
  "meta": "eNrjWsjIxV2UmlxapFdQlF+Sr9TJyMUSklpcIqTIxZqZl5daJMGowKjBbcStBxLV8wQJBUFkhCS5WEqAghJMYBWsYBVBYCEpKS4usFIwIcTDxZgINocziDFRSp2LFSIsx8WYCTVeAMl4iB2MmUlsYCcZAwC/Qiqb",
  "view": {
    "recursion": "bytes",
    "enum": "number"
  }
}@@;

$udfPar = Udf(Protobuf::Parse, $config as TypeConfig);
$udfSer = Udf(Protobuf::Serialize, $config as TypeConfig);

SELECT TestField, Ensure("Success", $udfPar(TestField) == $udfPar($udfSer($udfPar(TestField))), "Fail")
FROM plato.Input;

