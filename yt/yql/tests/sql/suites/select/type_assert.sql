/* syntax version 1 */
/* postgres can not */
/* syntax version 1 */
use plato;
select EnsureType(cast(key as Int64), Int64?, "some text 1") from Input;
select FormatType(EnsureType(TypeOf(1), Int32, "some text 2"));
select FormatType(EnsureType(TypeOf(1), Int32));

select EnsureConvertibleTo(cast(key as Int64), Double?, "some text 3") from Input;
select FormatType(EnsureConvertibleTo(TypeOf(1), Int64, "some text 4"));
select FormatType(EnsureConvertibleTo(TypeOf(1), Int64));
