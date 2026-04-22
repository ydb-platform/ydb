/* yt can not */
/* custom error: Failed to convert, type diff: Double!=Optional<Uint64> */
$list = ListFromRange(1, 101);

SELECT
    ListSampleN($list, CAST(10.0 AS Double)) AS test1
;
