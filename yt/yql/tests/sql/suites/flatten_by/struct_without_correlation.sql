/* syntax version 1 */
/* postgres can not */
pragma sampleselect;
use plato;

$data_dict = (select mod, aggregate_list(AsStruct(key as `struct`, subkey as subkey)) as list_struct from Input group by cast(key as uint32) % 10 as mod);

--insert into plato.Output
select
  mod, `struct`.`struct`
from $data_dict as dd
flatten by list_struct as `struct`
order by mod, column1;
--order by mod, iv, ls;
