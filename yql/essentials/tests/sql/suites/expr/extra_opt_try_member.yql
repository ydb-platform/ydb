/* postgres can not */
/* syntax version 1 */
/* yt can not */

$s = <|a:1, b:2u/1u, c:Just(Just(1))|>;
$js = Just($s);
$es = Nothing(Struct<a:Int32, b:Uint32?, c:Int32??>?);

-- TypeOf TryMember is type of third argument
-- field type should either match third type exactly, or (if the third type is optional) 
-- Optional(field) should be equal to third type
select
  TryMember($s, "b", Just(Just(99u))),
  TryMember($s, "c", Just(Just(Just(2)))),
  
  TryMember($js, "b", Just(Just(999u))),
  TryMember($js, "c", Just(Just(Just(999)))),
  
  TryMember($es, "b", Just(Just(999u))),
  TryMember($es, "c", Just(Just(Just(999)))),
;
  
