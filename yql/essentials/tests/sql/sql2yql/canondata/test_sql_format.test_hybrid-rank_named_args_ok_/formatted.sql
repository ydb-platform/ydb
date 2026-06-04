/* postgres can not */
/* custom error:HybridRank could not be rewritten*/
SELECT
    HybridRank(1.0, 2.0, ('idx_fulltext', 'idx_vector') AS Indexes, (10, 20) AS Limits, 60 AS K)
;
