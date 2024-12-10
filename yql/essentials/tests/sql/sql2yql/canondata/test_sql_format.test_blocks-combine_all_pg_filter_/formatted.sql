USE plato;

PRAGMA yt.UsePartitionsByKeysForFinalAgg = "false";

SELECT
    Pg::count(),
    Pg::count(a),
    Pg::sum(c),
    Pg::min(a),
    Pg::max(a),
    Pg::min(c),
    Pg::max(c),
    Pg::avg(c),
FROM
    Input
WHERE
    d == "aaa"
;
