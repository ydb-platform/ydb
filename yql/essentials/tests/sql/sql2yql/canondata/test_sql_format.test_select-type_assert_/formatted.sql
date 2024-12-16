/* syntax version 1 */
/* postgres can not */
/* syntax version 1 */
USE plato;

SELECT
    EnsureType(CAST(key AS Int64), Int64?, 'some text 1')
FROM
    Input
;

SELECT
    FormatType(EnsureType(TypeOf(1), Int32, 'some text 2'))
;

SELECT
    FormatType(EnsureType(TypeOf(1), Int32))
;

SELECT
    EnsureConvertibleTo(CAST(key AS Int64), Double?, 'some text 3')
FROM
    Input
;

SELECT
    FormatType(EnsureConvertibleTo(TypeOf(1), Int64, 'some text 4'))
;

SELECT
    FormatType(EnsureConvertibleTo(TypeOf(1), Int64))
;
