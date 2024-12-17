/* postgres can not */
/* custom error:Modification of "Output" view is not supported*/
INSERT INTO plato.Output WITH truncate
SELECT
    *
FROM
    plato.Input
;
