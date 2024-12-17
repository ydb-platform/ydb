/* postgres can not */
SELECT
    CAST(Yql::TzDate(AsAtom('1,UTC')) AS Uint8)
;

SELECT
    CAST(Yql::TzDate(AsAtom('1,UTC')) AS Uint32)
;

SELECT
    CAST(Yql::TzDate(AsAtom('1,UTC')) AS Int32)
;

SELECT
    CAST(Yql::TzDate(AsAtom('1,UTC')) AS Uint64)
;

SELECT
    CAST(Yql::TzDate(AsAtom('1,UTC')) AS Int64)
;

SELECT
    CAST(Yql::TzDatetime(AsAtom('1,UTC')) AS Uint8)
;

SELECT
    CAST(Yql::TzDatetime(AsAtom('1,UTC')) AS Uint32)
;

SELECT
    CAST(Yql::TzDatetime(AsAtom('1,UTC')) AS Int32)
;

SELECT
    CAST(Yql::TzDatetime(AsAtom('1,UTC')) AS Uint64)
;

SELECT
    CAST(Yql::TzDatetime(AsAtom('1,UTC')) AS Int64)
;

SELECT
    CAST(Yql::TzTimestamp(AsAtom('1,UTC')) AS Uint8)
;

SELECT
    CAST(Yql::TzTimestamp(AsAtom('1,UTC')) AS Uint32)
;

SELECT
    CAST(Yql::TzTimestamp(AsAtom('1,UTC')) AS Int32)
;

SELECT
    CAST(Yql::TzTimestamp(AsAtom('1,UTC')) AS Uint64)
;

SELECT
    CAST(Yql::TzTimestamp(AsAtom('1,UTC')) AS Int64)
;

SELECT
    CAST(1ut AS TzDate)
;

SELECT
    CAST(1u AS TzDate)
;

SELECT
    CAST(1 AS TzDate)
;

SELECT
    CAST(1ul AS TzDate)
;

SELECT
    CAST(1l AS TzDate)
;

SELECT
    CAST(-1 AS TzDate)
;

SELECT
    CAST(1 / 1 AS TzDate)
;

SELECT
    CAST(-1 / 1 AS TzDate)
;

SELECT
    CAST(1ut AS TzDatetime)
;

SELECT
    CAST(1u AS TzDatetime)
;

SELECT
    CAST(1 AS TzDatetime)
;

SELECT
    CAST(1ul AS TzDatetime)
;

SELECT
    CAST(1l AS TzDatetime)
;

SELECT
    CAST(-1 AS TzDatetime)
;

SELECT
    CAST(1 / 1 AS TzDatetime)
;

SELECT
    CAST(-1 / 1 AS TzDatetime)
;

SELECT
    CAST(1 / 0 AS TzDatetime)
;

SELECT
    CAST(1ut AS TzTimestamp)
;

SELECT
    CAST(1u AS TzTimestamp)
;

SELECT
    CAST(1 AS TzTimestamp)
;

SELECT
    CAST(1ul AS TzTimestamp)
;

SELECT
    CAST(1l AS TzTimestamp)
;

SELECT
    CAST(-1 AS TzTimestamp)
;

SELECT
    CAST(1 / 1 AS TzTimestamp)
;

SELECT
    CAST(-1 / 1 AS TzTimestamp)
;

SELECT
    CAST(1 / 0 AS TzTimestamp)
;
