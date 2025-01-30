/* postgres can not */
SELECT
    +Interval('P1D')
;

SELECT
    -Interval('P1D')
;

SELECT
    +Yql::Just(Interval('P1D'))
;

SELECT
    -Yql::Just(Interval('P1D'))
;

SELECT
    +Yql::Nothing(ParseType('Interval?'))
;

SELECT
    -Yql::Nothing(ParseType('Interval?'))
;
