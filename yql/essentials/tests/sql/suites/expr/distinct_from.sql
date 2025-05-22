/* syntax version 1 */
/* postgres can not */

select
    1 is distinct from 2,
    1 is not distinct from 2,
    null is distinct from null,
    Just(1 + 2) is distinct from Nothing(Int32?),
    Nothing(Int32??) is not distinct from Just(Nothing(Int32?))
;

