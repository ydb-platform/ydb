/* postgres can not */
/* syntax version 1 */
/* yt can not */

$s = <|a:1, b:2u/1u, c:Just(Just(1))|>;
$js = Just($s);
$es = Nothing(Struct<a:Int32, b:Uint32?, c:Int32??>?);

-- result of TryMember() when member is not present is third argument
-- (optional third arg if struct is optional, but third arg is not)
select
  TryMember($s, "z", 'qqq'),
  TryMember($js, "z", null),
  TryMember($es, "z", Just(Just('qqq'))),
  TryMember($js, "z", 'zzz'),
;


-- fully equivalent to <struct>.<name>
select
  TryMember($s, "a", null),
  TryMember($s, "b", null),
  TryMember($s, "c", null),

  TryMember($js, "a", null),
  TryMember($js, "b", null),
  TryMember($js, "c", null),

  TryMember($es, "a", null),
  TryMember($es, "b", null),
  TryMember($es, "c", null),
;

-- TypeOf TryMember is type of third argument
-- field type should either match third type exactly, or (if the third type is optional) 
-- Optional(field) should be equal to third type
select
  TryMember($s, "a", 999),
  TryMember($s, "a", Just(999)),
  TryMember($s, "b", Just(999u)),
  TryMember($s, "c", Just(Just(999))),
  
  TryMember($js, "a", 999),
  TryMember($js, "a", Just(999)),
  TryMember($js, "b", Just(999u)),
  TryMember($js, "c", Just(Just(999))),
  
  TryMember($es, "a", 999),
  TryMember($es, "a", Just(999)),
  TryMember($es, "b", Just(999u)),
  TryMember($es, "c", Just(Just(999))),
;
  
