/* postgres can not */
pragma direct_read;
select * from plato.Input order by key limit 100 offset 90;
