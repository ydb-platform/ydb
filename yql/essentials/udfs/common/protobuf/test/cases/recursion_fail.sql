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
    "recursion": "fail",
    "enum": "number"
  }
}@@;

$udf = Udf(Protobuf::Parse, $config as TypeConfig);

SELECT $udf(TestField) FROM plato.Input;
