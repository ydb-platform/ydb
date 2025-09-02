/* postgres can not */
SELECT
    Bool('true')
;

SELECT
    Bool('FalsE')
;

SELECT
    Int8('-128')
;

SELECT
    Int8('127')
;

SELECT
    Uint8('0')
;

SELECT
    Uint8('255')
;

SELECT
    Int16('-32768')
;

SELECT
    Int16('32767')
;

SELECT
    Uint16('0')
;

SELECT
    Uint16('65535')
;

SELECT
    Int32('-2147483648')
;

SELECT
    Int32('2147483647')
;

SELECT
    Uint32('0')
;

SELECT
    Uint32('4294967295')
;

SELECT
    Int64('-9223372036854775808')
;

SELECT
    Int64('9223372036854775807')
;

SELECT
    Uint64('0')
;

SELECT
    Uint64('18446744073709551615')
;

SELECT
    Float('0')
;

SELECT
    Float('1')
;

SELECT
    Float('-1e30')
;

SELECT
    Float('-inf')
;

SELECT
    Float('+inf')
;

SELECT
    Float('nan')
;

SELECT
    Double('0')
;

SELECT
    Double('1')
;

SELECT
    Double('-1e300')
;

SELECT
    Double('-inf')
;

SELECT
    Double('+inf')
;

SELECT
    Double('nan')
;

SELECT
    String('foo\xffbar')
;

SELECT
    Utf8('привет')
;

SELECT
    Yson('<a=1>[3;%false]')
;

SELECT
    Json(@@{"a":1,"b":null}@@)
;

SELECT
    CAST(Date('2000-01-01') AS string)
;

SELECT
    CAST(Datetime('2000-01-01T01:02:03Z') AS string)
;

SELECT
    CAST(Timestamp('2000-01-01T01:02:03.4Z') AS string)
;

SELECT
    CAST(Interval('P1DT12H') AS string)
;

SELECT
    TZDATE('2010-07-01,Europe/Moscow')
;

SELECT
    TZDATE('2010-07-01,America/Los_Angeles')
;

SELECT
    TZDATETIME('2010-07-01T00:00:00,Europe/Moscow')
;

SELECT
    TZTIMESTAMP('2010-07-01T00:00:00,America/Los_Angeles')
;

SELECT
    TZTIMESTAMP('2010-07-01T12:00:00.123456,Europe/Moscow')
;

SELECT
    Uuid('550e8400-e29b-41d4-a716-446655440000')
;
