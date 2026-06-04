/* postgres can not */
/* custom error:HybridRank expects at least 2 positional arguments*/
SELECT
    HybridRank(1.0, ('idx_fulltext', 'idx_vector') AS Indexes)
;
