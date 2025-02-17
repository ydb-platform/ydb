/*
syntax='proto3';

import "yt/yt_proto/yt/formats/extension.proto";

message Test {
  option (NYT.default_field_flags) = SERIALIZATION_YT;

  message Inner {
    string a = 1;
  }
  map<string, Inner> dict1 = 1 [(NYT.flags) = MAP_AS_DICT];
  map<string, Inner> dict2 = 2 [(NYT.flags) = MAP_AS_OPTIONAL_DICT];
  map<string, Inner> dict3 = 3 [(NYT.flags) = MAP_AS_LIST_OF_STRUCTS_LEGACY];
  map<string, Inner> dict4 = 4 [(NYT.flags) = MAP_AS_LIST_OF_STRUCTS];
  map<string, string> dict5 = 5 [(NYT.flags) = MAP_AS_DICT];
  map<string, string> dict6 = 6 [(NYT.flags) = MAP_AS_OPTIONAL_DICT];
  map<string, string> dict7 = 7 [(NYT.flags) = MAP_AS_LIST_OF_STRUCTS_LEGACY];
  map<string, string> dict8 = 8 [(NYT.flags) = MAP_AS_LIST_OF_STRUCTS];
}
*/

$config = @@{
  "name": "Test",
  "format": "json",
  "skip": 0,
  "lists": {
    "optional": false
  },
  "meta": "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",
  "view": {
    "recursion": "bytes",
    "enum": "number",
    "yt_mode": true
  }
}
@@;

$udfParse = Udf(Protobuf::Parse, $config as TypeConfig);
$udfSerialize = Udf(Protobuf::Serialize, $config as TypeConfig);

SELECT
    TestField,
    $udfParse(TestField),
    $udfSerialize($udfParse(TestField)),
    Ensure("Success", StablePickle($udfParse(TestField)) == StablePickle($udfParse($udfSerialize($udfParse(TestField)))), "Fail"),
FROM plato.Input;
