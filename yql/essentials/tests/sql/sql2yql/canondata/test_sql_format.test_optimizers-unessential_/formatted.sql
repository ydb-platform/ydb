PRAGMA warning('disable', '4510');

$input = AsList(AsStruct('key' AS key, 'value' AS value));

SELECT
    *
FROM
    AS_TABLE($input)
WHERE
    YQL::Unessential(key != 'not present', TRUE)
;
