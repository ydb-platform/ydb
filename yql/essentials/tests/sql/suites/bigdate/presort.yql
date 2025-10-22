pragma warning("disable","4510");
$wa1 = Date32("1900-01-01");
$wd1 = Datetime64("1900-01-01T02:03:04Z");
$wt1 = Timestamp64("1900-01-01T02:03:04.567891Z");

$waz1 = TzDate32("1900-01-01,Europe/Moscow");
$wdz1 = TzDatetime64("1900-01-01T02:03:04,Europe/Moscow");
$wtz1 = TzTimestamp64("1900-01-01T02:03:04.567891,Europe/Moscow");

$wa2 = Date32("1901-01-01");
$wd2 = Datetime64("1901-01-01T02:03:04Z");
$wt2 = Timestamp64("1901-01-01T02:03:04.567891Z");

$waz2 = TzDate32("1901-01-01,Europe/Moscow");
$wdz2 = TzDatetime64("1901-01-01T02:03:04,Europe/Moscow");
$wtz2 = TzTimestamp64("1901-01-01T02:03:04.567891,Europe/Moscow");

select 
cast(ListSortDesc([(Yql::Ascending($wa1),$wa1),(Yql::Ascending($wa2),$wa2)]) as List<Tuple<String,String>>),
cast(ListSortDesc([(Yql::Ascending($wd1),$wd1),(Yql::Ascending($wd2),$wd2)]) as List<Tuple<String,String>>),
cast(ListSortDesc([(Yql::Ascending($wt1),$wt1),(Yql::Ascending($wt2),$wt2)]) as List<Tuple<String,String>>),
cast(ListSortDesc([(Yql::Ascending($waz1),$waz1),(Yql::Ascending($waz2),$waz2)]) as List<Tuple<String,String>>),
cast(ListSortDesc([(Yql::Ascending($wdz1),$wdz1),(Yql::Ascending($wdz2),$wdz2)]) as List<Tuple<String,String>>),
cast(ListSortDesc([(Yql::Ascending($wtz1),$wtz1),(Yql::Ascending($wtz2),$wtz2)]) as List<Tuple<String,String>>);

