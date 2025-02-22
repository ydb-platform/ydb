use plato;
pragma yt.UsePartitionsByKeysForFinalAgg="false";

SELECT
    Pg::count(),
    Pg::count(a),
    Pg::sum(c),
    Pg::min(a),
    Pg::max(a),
    Pg::min(c),
    Pg::max(c),
    Pg::avg(c),
FROM Input
where d="aaa"