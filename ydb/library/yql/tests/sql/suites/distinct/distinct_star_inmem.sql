/* syntax version 1 */
/* postgres can not */

$src = select 1, 2, 3 union all select 1, 2, 3;

select distinct * from $src;
