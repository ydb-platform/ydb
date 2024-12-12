/* syntax version 1 */
/* postgres can not */
SELECT
    aggregate_list(CAST(value AS int)) AS val_list
FROM
    plato.Input
;
