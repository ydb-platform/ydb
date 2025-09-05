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
    Pg::avg(PgCast(1p,pgint4)),
    Pg::avg(PgCast(1p,pgint8)),
    Pg::avg(PgCast(1p,pgfloat8)),
    Pg::regr_count(1.0p,1.0p)
FROM Input
