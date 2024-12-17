/* syntax version 1 */
/* postgres can not */
USE plato;

DEFINE SUBQUERY $s($_i) AS
    $t = (
        SELECT
            AGGREGATE_LIST(Path)
        FROM
            FOLDER('')
        WHERE
            Path LIKE 'Input%'
    );

    SELECT
        *
    FROM
        EACH($t)
    ;
END DEFINE;

$extractor = SubqueryMergeFor([1], $s);

SELECT
    *
FROM
    $extractor()
;
