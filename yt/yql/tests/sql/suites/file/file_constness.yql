/* postgres can not */
/* syntax version 1 */
select key,
       FileContent("keyid.lst") as content,
       ListCollect(ParseFile('int32', "keyid.lst")) as content_list,
from plato.Input group by key order by key;
