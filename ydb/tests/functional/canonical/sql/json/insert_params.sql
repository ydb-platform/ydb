--!syntax_v1

DECLARE $data AS List<Struct<Key: Int32, Value: JsonDocument>>;

INSERT INTO ResultParamsJD
SELECT Key, Value FROM AS_TABLE($data);
