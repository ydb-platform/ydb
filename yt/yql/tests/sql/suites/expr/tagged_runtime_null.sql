/* postgres can not */
/* dqfile can not */ /* See details in YQL-20113#68ac0bd952cebb2ea9576cb3 */

use plato;
insert into @tmp
select Just((
    AsTagged(null,"C"),
    )) as x;
commit;
select x.0 from @tmp
