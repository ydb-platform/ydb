--!syntax_v1

DECLARE $data AS List<Struct<Key: DyNumber, Value: Json>>;

INSERT INTO ResultParamsDyNumber
SELECT Key, Value FROM AS_TABLE($data);
