$config = @@{
  "name": "EmptyMessage",
  "meta": "",
}@@;

$udf = Udf(Protobuf::TryParse, $config as TypeConfig);
SELECT $udf(col) FROM AS_TABLE([<|col: ""|>]);
