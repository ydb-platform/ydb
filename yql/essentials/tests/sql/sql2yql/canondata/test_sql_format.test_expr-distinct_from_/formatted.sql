/* syntax version 1 */
/* postgres can not */
SELECT
    1 IS DISTINCT FROM 2,
    1 IS NOT DISTINCT FROM 2,
    NULL IS DISTINCT FROM NULL,
    Just(1 + 2) IS DISTINCT FROM Nothing(Int32?),
    Nothing(Int32??) IS NOT DISTINCT FROM Just(Nothing(Int32?))
;
