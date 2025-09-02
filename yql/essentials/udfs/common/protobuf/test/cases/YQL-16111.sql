/*
syntax='proto3';

import "yt/yt_proto/yt/formats/extension.proto";

message Test {
  option (NYT.default_field_flags) = SERIALIZATION_YT;

  message InnerSubProto {
      option (NYT.default_field_flags) = SERIALIZATION_YT;
      int64 x = 1;
      string y = 2;

      message TSubField {
          repeated string List = 1;
      }

      TSubField SubField = 3 [(NYT.flags) = SERIALIZATION_PROTOBUF];
  }

  map<string, InnerSubProto> dict = 10 [(NYT.flags) = MAP_AS_DICT];
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

$data = @@
{
    "dict": [
        {
            "key": "key2",
            "value": {
                "x": 23,
                "y": "yy",
                "SubField": {"List": ["s1"]}
            }
        }
    ]
}
@@;

SELECT
    $data,
    $udfParse($data),
    $udfSerialize($udfParse($data)),
    Ensure("Success", StablePickle($udfParse($data)) == StablePickle($udfParse($udfSerialize($udfParse($data)))), "Fail")
;
