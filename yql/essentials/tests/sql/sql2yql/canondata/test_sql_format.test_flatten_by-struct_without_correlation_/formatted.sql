/* syntax version 1 */
/* postgres can not */
PRAGMA sampleselect;
USE plato;

$data_dict = (
    SELECT
        mod,
        aggregate_list(AsStruct(key AS `struct`, subkey AS subkey)) AS list_struct
    FROM
        Input
    GROUP BY
        CAST(key AS uint32) % 10 AS mod
);

--insert into plato.Output
SELECT
    mod,
    `struct`.`struct`
FROM
    $data_dict AS dd
    FLATTEN BY list_struct AS `struct`
ORDER BY
    mod,
    column1
;
--order by mod, iv, ls;
