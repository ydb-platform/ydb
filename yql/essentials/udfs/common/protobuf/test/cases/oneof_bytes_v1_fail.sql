/*

syntax='proto3';

message Test {
  oneof Var {
    Test test = 1;
    string str = 2;
  }
};

*/

$config = @@{
  "name": "Test",
  "format": "json",
  "skip": 0,
  "lists": {
    "optional": false
  },
  "meta": "H4sIAAAAAAAAA+OK4xIpyS3Qs/ArMkhMCsrwN9UrKMovyVey42IJSS0uEZLmYikB0hKMCowa3EaseiBBD4YgsKCQEBdzcUmRBBNQjhMoCOI4sXIxhyUWJbGBjTEGACyKGj9gAAAA",
  "view": {
    "recursion": "bytes",
    "enum": "number"
  }
}@@;

$udfPar = Udf(Protobuf::Parse, $config as TypeConfig);
$udfSer = Udf(Protobuf::Serialize, $config as TypeConfig);

SELECT TestField, $udfPar(TestField), $udfSer($udfPar(TestField)) FROM plato.Input;

