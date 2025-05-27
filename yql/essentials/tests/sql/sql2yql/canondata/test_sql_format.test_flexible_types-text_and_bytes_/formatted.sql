/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
PRAGMA FlexibleTypes;

$src = (
    SELECT
        Text('test_text, привет') AS text,
        Bytes('binary\x00\xff') AS bytes
);

SELECT
    text,
    bytes,
    len(bytes) AS bytes_len,
    FormatType(TypeOf(text)) AS text_real_type,
    FormatType(Bytes) AS bytes_real_type,
FROM
    $src
;
