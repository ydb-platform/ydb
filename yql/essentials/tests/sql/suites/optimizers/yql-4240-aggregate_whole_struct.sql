/* postgres can not */
use plato;

$data = (select value as attr, key as urlBase, cast(subkey as int32) as dupsCount from Input0);

        SELECT
            urlBase,
            SUM(dupsCount) as allDocs,
            MAX_BY(AsStruct(dupsCount as dupsCount, attr as attr), dupsCount) as best
        FROM (
            SELECT urlBase, attr, count(*) as dupsCount
            FROM $data
            GROUP BY urlBase, attr
        )
        GROUP BY urlBase
        ORDER BY urlBase
