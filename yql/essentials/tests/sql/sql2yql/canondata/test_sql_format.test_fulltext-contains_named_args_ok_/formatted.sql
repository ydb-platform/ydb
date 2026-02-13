/* postgres can not */
/* custom error:Fulltext contains is not implemented yet*/
SELECT
    FulltextContains('some text', 'text', 'or' AS Mode, '1' AS MinimumShouldMatch)
;
