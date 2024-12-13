/* postgres can not */
SELECT
    *
FROM
    plato.Input
WHERE
    key IN YQL::DictFromKeys(ParseType("String"), AsTuple("075", "023"))
ORDER BY
    key
;
