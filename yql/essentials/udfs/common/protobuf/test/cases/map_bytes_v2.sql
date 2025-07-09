/*

syntax='proto3';

message Test {
  map<string, string> map1 = 1;
  map<string, Test> map2 = 2;
};

*/

$config = @@{
  "name": "Test",
  "format": "json",
  "skip": 0,
  "lists": {
    "optional": false
  },
  "meta": "H4sIAAAAAAAAA+N6xcglUpJboOec457pmJVVUemmV1CUX5KvdJKRiyUktbhESJmLJTexwFCCUYFZg9uIXw8kqOcLFHHNKymqDAJLQhUZSTChKTJCKDKSMufihOsTEuBizk6tBJrKqMEZBGIKiXCxliXmlKYCDQGJQThWTBaMUnZgjUa4NEoja+Q2YgXbjqQ/iQ3sJWMARIs/0+0AAAA=",
  "view": {
    "recursion": "bytesV2",
    "enum": "number"
  }
}@@;

$udfPar = Udf(Protobuf::Parse, $config as TypeConfig);
$udfSer = Udf(Protobuf::Serialize, $config as TypeConfig);

SELECT TestField, $udfPar(TestField), $udfSer($udfPar(TestField)) FROM plato.Input;

