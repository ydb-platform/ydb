/* syntax version 1 */
SELECT
    JsonGet::json_get('{"user":{"name":"alice"},"count":42}', "user.name") AS nested_name,
    JsonGet::json_get('{"user":{"name":"alice"},"count":42}', "count") AS count;
