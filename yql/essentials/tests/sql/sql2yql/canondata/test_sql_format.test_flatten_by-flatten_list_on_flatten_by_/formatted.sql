/* syntax version 1 */
USE plato;

$flatten = (
    SELECT
        answer_and_clicks.0 AS permalink,
        bc_type_tuple
    FROM
        Input AS a
        FLATTEN BY parsed_answers_and_clicks AS answer_and_clicks
    WHERE
        answer_and_clicks.1 == 1
);

SELECT
    bc_type,
    permalink,
FROM (
    SELECT
        asList(bc_type_tuple.0, 'total') AS bc_type,
        a.*
    WITHOUT
        bc_type_tuple
    FROM
        $flatten AS a
)
    FLATTEN LIST BY bc_type
;
