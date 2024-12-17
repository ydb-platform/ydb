USE plato;

$waz = TzDate32('1900-01-01,Europe/Moscow');
$wdz = TzDatetime64('1900-01-01T02:03:04,Europe/Moscow');
$wtz = TzTimestamp64('1900-01-01T02:03:04.567891,Europe/Moscow');

INSERT INTO Output
SELECT
    $waz AS waz,
    $wdz AS wdz,
    $wtz AS wtz,
    ($waz, $wdz, $waz) AS tup
;
