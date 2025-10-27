use plato;

$waz = TzDate32("1900-01-01,Europe/Moscow");
$wdz = TzDatetime64("1900-01-01T02:03:04,Europe/Moscow");
$wtz = TzTimestamp64("1900-01-01T02:03:04.567891,Europe/Moscow");

insert into Output
select 
    $waz as waz,
    $wdz as wdz,
    $wtz as wtz,
    ($waz, $wdz, $waz) as tup;
