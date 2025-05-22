/* syntax version 1 */
/* postgres can not */
select ListExtend(String::SplitToList("1234 123", " "),String::SplitToList("1234 123", " "))[1];
