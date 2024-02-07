/* syntax version 1 */
/* postgres can not */
/* hybridfile can not YQL-17764 */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 3 */

use plato;
pragma DisableSimpleColumns;

$a = select * from Input where key > "199" and value != "bbb";

select * from (select a.value from Input as a inner join $a as b using(subkey)) tablesample bernoulli(40);
