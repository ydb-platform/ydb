/* syntax version 1 */
/* postgres can not */

select
  Null is not distinct from 1/0,               --true
  1/0 is distinct from Null,                   --false
  1u/0u  is distinct from 1/0,                 --false
  Just(1u) is not distinct from 1/0,           --false
  1u/0u is distinct from Just(1),              --true
  1u is distinct from 1,                       --false
  Nothing(Int32??) is distinct from Just(1/0), --true
  1 is not distinct from Just(Just(1u)),       --true
;
