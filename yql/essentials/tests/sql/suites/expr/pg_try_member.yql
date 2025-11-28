/* postgres can not */
/* syntax version 1 */
/* yt can not */

$s = <|a:1p|>;
$js = Just($s);
$es = Nothing(Struct<a:PgInt>?);

-- fully equivalent to <struct>.<name>
select
  TryMember($s, "a", null),
  TryMember($js, "a", null),
  TryMember($es, "a", null),
;

-- TypeOf TryMember is type of third argument
-- field type should either match third type exactly, or (if the third type is optional) 
-- Optional(field) should be equal to third type
select
  TryMember($s, "a", 999p),
  TryMember($s, "a", Just(999p)),

  TryMember($js, "a", 999p),
  TryMember($js, "a", Just(999p)),

  TryMember($es, "a", 999p),
  TryMember($es, "a", Just(999p)),
;

