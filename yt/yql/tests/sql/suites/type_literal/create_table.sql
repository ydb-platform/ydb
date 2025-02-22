/* syntax version 1 */
/* do not execute */

create table plato.Output (
    c_void Void,
    c_unit Unit,
    c_bool Bool,

    c_uint8 Uint8,
    c_uint16 Uint16,
    c_uint32 Uint32 NOT NULL,
    c_uint64 Uint64 NOT NULL,

    c_int8 int8,
    c_int16 int16,
    c_int32 int32,
    c_int64 int64,

    c_tinyint TINYINT,
    c_smallint SMALLINT,
    c_int INT,
    c_integer INTEGER,
    c_bigint BIGINT,

    c_float float,
    c_double double,

    c_string String,
    c_varchar Varchar,
    c_utf8 Utf8,
    c_yson Yson,
    c_json Json,
    c_uuid Uuid,

    c_date Date,
    c_datetime Datetime,
    c_timestamp Timestamp,
    c_interval Interval,
    c_tzdate TzDate,
    c_tzdatetime TzDatetime,
    c_tztimestamp TzTimestamp,

    c_decimal0 Decimal(20, 10),

    c_optional0 Optional<string>,
    c_optional1 string ?,
    c_optional2 string??,
    c_optional3 string? ? ?,
    c_optional4 optional<string>?,

    c_tuple Tuple< bool, uint64 >,
    c_struct Struct< foo:string, 'bar':float >,
    c_variant1 Variant< int, bool >,
    c_variant2 Variant< foo:int, "bar":bool >,

    c_list List<Yson>,
    c_stream Stream<Date>,
    c_flow Flow<Uuid>,
    c_dict Dict<string, interval>,
    c_set Set<TzTimestamp>,
    c_enum Enum< first, second, third >,

    c_resource0 Resource<'foo'>,
    c_resource1 Resource<bar>,

    c_tagged Tagged<Uint64, 'tag'>,
    c_generic Generic,

    c_sometype0 set<list<dict<string, int??>?>>,

    c_callable0 Callable<()->bool>,
    c_callable1 Callable<(int32)->int8>,
    c_callable2 Callable<(int32, string)->float>,
    c_callable3 Callable<(int32, foo : string)->float>,
    c_callable4 Callable<(foo : int32, bar : string)->float>,
    c_callable5 Callable<([foo : int32, bar : string])->float>,
    c_callable6 Callable<(foo : int32, [bar : string])->float>,
    c_callable7 Callable<(foo : int32, bar : optional<string>)->float?>,
    c_callable8 Callable<(foo : int32{automap}, bar : string{Automap})->float>
);
