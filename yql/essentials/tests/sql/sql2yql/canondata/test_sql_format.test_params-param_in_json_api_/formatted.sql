/* syntax version 1 */
/* postgres can not */
DECLARE $input_json AS Json;
DECLARE $int64_param AS Int64;
DECLARE $double_param AS Double;
DECLARE $bool_param AS Bool;
DECLARE $string_param AS Utf8;
DECLARE $json_param AS Json;

SELECT
    JSON_VALUE (
        $input_json,
        'strict $var' PASSING $int64_param AS var RETURNING Int64
    ),
    JSON_VALUE (
        $input_json,
        'strict $var' PASSING $double_param AS var RETURNING Double
    ),
    JSON_VALUE (
        $input_json,
        'strict $var' PASSING $bool_param AS var RETURNING Bool
    ),
    JSON_VALUE (
        $input_json,
        'strict $var' PASSING $string_param AS var RETURNING String
    ),
    JSON_QUERY (
        $input_json,
        'strict $var' PASSING $json_param AS var
    )
;
