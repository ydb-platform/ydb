/*
import "yt/yt_proto/yt/formats/extension.proto";

message Test {
    enum Color
    {
        WHITE = 0;
        BLUE = 1;
        RED = -1;
    }

    required Color ColorYtIntField = 1 [(NYT.flags) = ENUM_INT];
    required Color ColorYtStringField = 2 [(NYT.flags) = ENUM_STRING];
    required Color ColorField = 3;
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
    "recursion": "fail",
    "enum": "number",
    "yt_mode": true
  }
}
@@;

$udfParse = Udf(Protobuf::Parse, $config as TypeConfig);
$udfSerialize = Udf(Protobuf::Serialize, $config as TypeConfig);

$data = @@
{
    "ColorYtIntField": 1,
    "ColorYtStringField": "RED",
    "ColorField": 0
}
@@;

SELECT
    $data,
    $udfParse($data),
    $udfSerialize($udfParse($data)),
    Ensure("Success", StablePickle($udfParse($data)) == StablePickle($udfParse($udfSerialize($udfParse($data)))), "Fail")
;
