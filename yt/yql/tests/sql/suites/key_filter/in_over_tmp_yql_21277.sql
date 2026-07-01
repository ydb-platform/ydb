/* postgres can not */
USE plato;

$config = Yson::ConvertTo(
    Yson::ParseJson(@@{"filter_users": true}@@),
    Struct<filter_users: Bool>
);

INSERT INTO @keys
SELECT key FROM Keys WHERE key IS NOT NULL;

COMMIT;

DEFINE SUBQUERY $keys() AS
    SELECT key FROM @keys;
END DEFINE;

$keys_processed = PROCESS $keys();

DEFINE SUBQUERY $filtered() AS
    SELECT key, subkey, value
    FROM EACH(AsList("Input1", "Input2"))
    WHERE (NOT $config.filter_users OR key IN $keys_processed)
END DEFINE;

INSERT INTO @out WITH TRUNCATE
SELECT key, subkey, value FROM $filtered();

COMMIT;

SELECT * FROM @out ORDER BY key, subkey, value;
