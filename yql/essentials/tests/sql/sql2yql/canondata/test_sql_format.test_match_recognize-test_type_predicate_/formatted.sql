/* custom error:DEFINE expression must be a predicate*/
PRAGMA FeatureR010 = 'prototype';
PRAGMA config.flags('MatchRecognizeStream', 'disable');

$data = [<||>];

$BadPredicate = (
    SELECT
        TableRow()
    FROM (
        SELECT
            *
        FROM
            AS_TABLE($data) MATCH_RECOGNIZE (
                ONE ROW PER MATCH
                AFTER MATCH SKIP TO NEXT ROW
                PATTERN (A)
                DEFINE
                    A AS 123 -- must fail, Bool expected
            )
    )
);

SELECT
    FormatType(TypeOf($BadPredicate))
;
