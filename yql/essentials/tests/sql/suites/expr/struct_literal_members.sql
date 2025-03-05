/* postgres can not */
$list = YQL::@@
(AddMember (AddMember (Struct) 'query (String 'QUERY)) 'result (String 'RESULT))
@@;
select $list.query as query, $list.result as result;
