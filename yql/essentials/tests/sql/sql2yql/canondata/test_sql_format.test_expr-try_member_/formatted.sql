/* postgres can not */
/* syntax version 1 */
/* yt can not */
$s = <|a: 1, b: 2u / 1u, c: Just(Just(1))|>;
$js = Just($s);
$es = Nothing(Struct<a: Int32, b: Uint32?, c: Int32??>?);

-- result of TryMember() when member is not present is third argument
-- (optional third arg if struct is optional, but third arg is not)
SELECT
    TryMember($s, 'z', 'qqq'),
    TryMember($js, 'z', NULL),
    TryMember($es, 'z', Just(Just('qqq'))),
    TryMember($js, 'z', 'zzz'),
;

-- fully equivalent to <struct>.<name>
SELECT
    TryMember($s, 'a', NULL),
    TryMember($s, 'b', NULL),
    TryMember($s, 'c', NULL),
    TryMember($js, 'a', NULL),
    TryMember($js, 'b', NULL),
    TryMember($js, 'c', NULL),
    TryMember($es, 'a', NULL),
    TryMember($es, 'b', NULL),
    TryMember($es, 'c', NULL),
;

-- TypeOf TryMember is type of third argument
-- field type should either match third type exactly, or (if the third type is optional) 
-- Optional(field) should be equal to third type
SELECT
    TryMember($s, 'a', 999),
    TryMember($s, 'a', Just(999)),
    TryMember($s, 'b', Just(999u)),
    TryMember($s, 'c', Just(Just(999))),
    TryMember($js, 'a', 999),
    TryMember($js, 'a', Just(999)),
    TryMember($js, 'b', Just(999u)),
    TryMember($js, 'c', Just(Just(999))),
    TryMember($es, 'a', 999),
    TryMember($es, 'a', Just(999)),
    TryMember($es, 'b', Just(999u)),
    TryMember($es, 'c', Just(Just(999))),
;
