/* postgres can not */
$list = YQL::@@
(AddMember (AddMember (Struct) 'query (String 'QUERY)) 'result (String 'RESULT))
@@;

SELECT
    $list.query AS query,
    $list.result AS result
;
