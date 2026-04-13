/* syntax version 1 */
/* postgres can not */
SELECT
    Nothing(pgint8) IS NULL,
    Nothing(pgint8) IS NOT NULL,
    Opaque(Nothing(pgint8)) IS NULL,
    Opaque(Nothing(pgint8)) IS NOT NULL
;

SELECT
    1p IS NULL,
    1p IS NOT NULL,
    Opaque(1p) IS NULL,
    Opaque(1p) IS NOT NULL
;
