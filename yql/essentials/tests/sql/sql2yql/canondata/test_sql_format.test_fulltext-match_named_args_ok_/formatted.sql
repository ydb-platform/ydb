/* postgres can not */
/* custom error:Fulltext match is not implemented yet*/
SELECT
    FulltextMatch('some text', 'text', 'or' AS DefaultOperator, '1' AS MinimumShouldMatch)
;
