/* postgres can not */
/* custom error:Fulltext score is not implemented yet*/
SELECT
    FulltextScore('some text', 'text', 'or' AS Mode, '1' AS MinimumShouldMatch)
;
