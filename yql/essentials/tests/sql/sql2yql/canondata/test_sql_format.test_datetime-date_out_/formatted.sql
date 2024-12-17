/* postgres can not */
SELECT
    CAST(CAST(18000u AS date) AS string)
;

SELECT
    CAST(CAST(18000u * 86400ul + 1u * 3600u + 2u * 60u + 3u AS datetime) AS string)
;

SELECT
    CAST(CAST(18000u * 86400000000ul + 1u * 3600000000ul + 2u * 60000000ul + 3000000ul + 4u AS timestamp) AS string)
;

SELECT
    CAST(CAST(18000u * 86400000000ul + 1u * 3600000000ul + 2u * 60000000ul + 3000000ul + 4u AS interval) AS string)
;

SELECT
    CAST(CAST(49673u - 1u AS date) AS string)
;

SELECT
    CAST(CAST(49673u AS date) AS string)
;

SELECT
    CAST(CAST(49673u * 86400ul - 1u AS datetime) AS string)
;

SELECT
    CAST(CAST(49673u * 86400ul AS datetime) AS string)
;

SELECT
    CAST(CAST(49673u * 86400000000ul - 1u AS timestamp) AS string)
;

SELECT
    CAST(CAST(49673u * 86400000000ul AS timestamp) AS string)
;

SELECT
    CAST(CAST(49673u * 86400000000ul - 1u AS interval) AS string)
;

SELECT
    CAST(CAST(49673u * 86400000000ul AS interval) AS string)
;
