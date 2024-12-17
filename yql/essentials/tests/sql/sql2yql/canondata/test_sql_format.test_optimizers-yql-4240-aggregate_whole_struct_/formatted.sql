/* postgres can not */
USE plato;

$data = (
    SELECT
        value AS attr,
        key AS urlBase,
        CAST(subkey AS int32) AS dupsCount
    FROM
        Input0
);

SELECT
    urlBase,
    SUM(dupsCount) AS allDocs,
    MAX_BY(AsStruct(dupsCount AS dupsCount, attr AS attr), dupsCount) AS best
FROM (
    SELECT
        urlBase,
        attr,
        count(*) AS dupsCount
    FROM
        $data
    GROUP BY
        urlBase,
        attr
)
GROUP BY
    urlBase
ORDER BY
    urlBase
;
