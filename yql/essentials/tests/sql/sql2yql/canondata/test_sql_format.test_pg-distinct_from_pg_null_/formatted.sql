/* syntax version 1 */
/* postgres can not */
SELECT
    Nothing(pgint8) IS DISTINCT FROM NULL,
    Nothing(pgint8) IS NOT DISTINCT FROM NULL,
    NULL IS DISTINCT FROM Nothing(pgint8),
    NULL IS NOT DISTINCT FROM Nothing(pgint8),
    Opaque(Nothing(pgint8)) IS DISTINCT FROM NULL,
    Opaque(Nothing(pgint8)) IS NOT DISTINCT FROM NULL,
    NULL IS DISTINCT FROM Opaque(Nothing(pgint8)),
    NULL IS NOT DISTINCT FROM Opaque(Nothing(pgint8))
;

SELECT
    1p IS DISTINCT FROM NULL,
    1p IS NOT DISTINCT FROM NULL,
    NULL IS DISTINCT FROM 1p,
    NULL IS NOT DISTINCT FROM 1p,
    Opaque(1p) IS DISTINCT FROM NULL,
    Opaque(1p) IS NOT DISTINCT FROM NULL,
    NULL IS DISTINCT FROM Opaque(1p),
    NULL IS NOT DISTINCT FROM Opaque(1p)
;
