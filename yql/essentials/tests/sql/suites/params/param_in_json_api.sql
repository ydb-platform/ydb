/* syntax version 1 */
/* postgres can not */

DECLARE $input_json as Json;
DECLARE $int64_param as Int64;
DECLARE $double_param as Double;
DECLARE $bool_param as Bool;
DECLARE $string_param as Utf8;
DECLARE $json_param as Json;

SELECT
    JSON_VALUE(
        $input_json,
        "strict $var"
        PASSING
            $int64_param as var
        RETURNING Int64
    ),
    JSON_VALUE(
        $input_json,
        "strict $var"
        PASSING
            $double_param as var
        RETURNING Double
    ),
    JSON_VALUE(
        $input_json,
        "strict $var"
        PASSING
            $bool_param as var
        RETURNING Bool
    ),
    JSON_VALUE(
        $input_json,
        "strict $var"
        PASSING
            $string_param as var
        RETURNING String
    ),
    JSON_QUERY(
        $input_json,
        "strict $var"
        PASSING
            $json_param as var
    );