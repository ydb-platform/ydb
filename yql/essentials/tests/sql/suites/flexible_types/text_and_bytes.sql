/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
pragma FlexibleTypes;


$src = select Text("test_text, привет") as text, Bytes("binary\x00\xff") as bytes;

select
  text,
  bytes,
  len(bytes) as bytes_len,
  FormatType(TypeOf(text)) as text_real_type,
  FormatType(Bytes) as bytes_real_type,
from $src;

