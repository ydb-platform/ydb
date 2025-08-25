/*
syntax='proto2';

message Test {
  message InnerInner {
    required string a = 1;
  }

  message Inner {
    required InnerInner i = 1;
  }

  optional Inner inner = 1;

  // Simple optional recursion.
  optional Test test = 2;

  // Repeated recursion.
  repeated Test repeated = 4;

  // Just a number.
  required int64 number = 6;

  message InnerMessage {
    // Required recursion via optional message.
    required Test req = 1;
  }

  optional InnerMessage req = 7;
}
*/

$config = @@{
  "name": "Test",
  "format": "json",
  "skip": 0,
  "lists": {
    "optional": false
  },
  "meta": "H4sIAAAAAAAAA02PvQ6CMBSF0/KjXhhMYxQZDBoTmRh4BRcdXBoWxxoqIYYfS3kSX8OHtLRGWZr0O+eecy+8MSxk1SbX7F48js2pz5NWNLLZvTDYGe8k2YJT1jUXAYpQ7KVeMtDkPCBqFLIGWyoYYO1wtINqpKangrecSZ4HdmT95R8mS3DrvrqpBjfCsUW/P7IHS/BnMNGhZFR74V3HCk4HOQwBNNQP8QExtSiOZxSx8ACOwRtApcZeOh8FmSNQqYz+OJmsTLWZ+C48kA+TG9kxLwEAAA==",
  "view": {
    "recursion": "bytesV2",
    "enum": "number"
  }
}@@;

$configOptList = @@{
  "name": "Test",
  "format": "json",
  "skip": 0,
  "lists": {
    "optional": true
  },
  "meta": "H4sIAAAAAAAAA02PvQ6CMBSF0/KjXhhMYxQZDBoTmRh4BRcdXBoWxxoqIYYfS3kSX8OHtLRGWZr0O+eecy+8MSxk1SbX7F48js2pz5NWNLLZvTDYGe8k2YJT1jUXAYpQ7KVeMtDkPCBqFLIGWyoYYO1wtINqpKangrecSZ4HdmT95R8mS3DrvrqpBjfCsUW/P7IHS/BnMNGhZFR74V3HCk4HOQwBNNQP8QExtSiOZxSx8ACOwRtApcZeOh8FmSNQqYz+OJmsTLWZ+C48kA+TG9kxLwEAAA==",
  "view": {
    "recursion": "bytesV2",
    "enum": "number"
  }
}@@;

$udfPar = Udf(Protobuf::Parse, $config as TypeConfig);
$udfSer = Udf(Protobuf::Serialize, $config as TypeConfig);

$udfParOpt = Udf(Protobuf::Parse, $configOptList as TypeConfig);
$udfSerOpt = Udf(Protobuf::Serialize, $configOptList as TypeConfig);

SELECT TestField, $udfPar(TestField), $udfSer($udfPar(TestField)), $udfParOpt(TestField), $udfSerOpt($udfParOpt(TestField))
FROM plato.Input;

