/* syntax version 1 */
/* postgres can not */
SELECT
    NULL IS NOT DISTINCT FROM 1 / 0, --true
    1 / 0 IS DISTINCT FROM NULL, --false
    1u / 0u IS DISTINCT FROM 1 / 0, --false
    Just(1u) IS NOT DISTINCT FROM 1 / 0, --false
    1u / 0u IS DISTINCT FROM Just(1), --true
    1u IS DISTINCT FROM 1, --false
    Nothing(Int32??) IS DISTINCT FROM Just(1 / 0), --true
    1 IS NOT DISTINCT FROM Just(Just(1u)), --true
;
