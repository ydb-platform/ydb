/* postgres can not */
USE plato;

$identity = ($x) -> {
    RETURN $x;
};

$idDate32 = CALLABLE (Callable<(date32) -> date32>, $identity);
$idDatetime64 = CALLABLE (Callable<(datetime64) -> datetime64>, $identity);
$idTimestamp64 = CALLABLE (Callable<(timestamp64) -> timestamp64>, $identity);
$idInterval64 = CALLABLE (Callable<(interval64) -> interval64>, $identity);
$valDate = unwrap(CAST(1 AS date));
$valDate32 = unwrap(CAST(-1 AS date32));
$valDatetime = unwrap(CAST(86400 AS datetime));
$valDatetime64 = unwrap(CAST(-86400 AS datetime64));
$valTimestamp = unwrap(CAST(86400l * 1000000 AS timestamp));
$valTimestamp64 = unwrap(CAST(86400l * 1000000 AS timestamp64));
$valInterval = unwrap(CAST(1 AS interval));
$valInterval64 = unwrap(CAST(-1 AS interval64));

SELECT
    1,
    $idDate32($valDate),
    $idDate32($valDate32),
    2,
    $idDatetime64($valDate),
    $idDatetime64($valDate32),
    $idDatetime64($valDatetime),
    $idDatetime64($valDatetime64),
    3,
    $idTimestamp64($valDate),
    $idTimestamp64($valDate32),
    $idTimestamp64($valDatetime),
    $idTimestamp64($valDatetime64),
    $idTimestamp64($valTimestamp),
    $idTimestamp64($valTimestamp64),
    4,
    $idInterval64($valInterval),
    $idInterval64($valInterval64)
;

SELECT
    row,
    1,
    $idTimestamp64(d32),
    $idDatetime64(d32),
    $idDate32(d32),
    2,
    $idTimestamp64(dt64),
    $idDatetime64(dt64),
    3,
    $idTimestamp64(ts64),
    4,
    $idInterval64(i64)
FROM
    BigDates
ORDER BY
    row
;

SELECT
    row,
    1,
    $idTimestamp64(d),
    $idDatetime64(d),
    $idDate32(d),
    2,
    $idTimestamp64(dt),
    $idDatetime64(dt),
    3,
    $idTimestamp64(ts)
FROM
    NarrowDates
ORDER BY
    row
;

SELECT
    row,
    $idInterval64(i)
FROM
    NarrowInterval
ORDER BY
    row
;
