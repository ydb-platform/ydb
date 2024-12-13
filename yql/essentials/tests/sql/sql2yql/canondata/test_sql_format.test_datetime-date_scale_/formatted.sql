/* postgres can not */
SELECT
    CAST(CAST(1u AS date) AS datetime)
;

SELECT
    CAST(CAST(1u AS date) AS timestamp)
;

SELECT
    CAST(CAST(86400u AS datetime) AS timestamp)
;

SELECT
    CAST(CAST(86400u AS datetime) AS date)
;

SELECT
    CAST(CAST(86400000000ul AS timestamp) AS date)
;

SELECT
    CAST(CAST(86400000000ul AS timestamp) AS datetime)
;
