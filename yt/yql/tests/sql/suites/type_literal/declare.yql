/* syntax version 1 */
/* do not execute */

declare $c_void as Void;
declare $c_unit as Unit;
declare $c_bool as Bool;

declare $c_uint8 as Uint8;
declare $c_uint16 as Uint16;
declare $c_uint32 as Uint32;
declare $c_uint64 as Uint64;

declare $c_int8 as int8;
declare $c_int16 as int16;
declare $c_int32 as int32;
declare $c_int64 as int64;

declare $c_tinyint as TINYINT;
declare $c_smallint as SMALLINT;
declare $c_int as INT;
declare $c_integer as INTEGER;
declare $c_bigint as BIGINT;

declare $c_float as float;
declare $c_double as double;

declare $c_string as String;
declare $c_varchar as Varchar;
declare $c_utf8 as Utf8;
declare $c_yson as Yson;
declare $c_json as Json;
declare $c_uuid as Uuid;

declare $c_date as Date;
declare $c_datetime as Datetime;
declare $c_timestamp as Timestamp;
declare $c_interval as Interval;
declare $c_tzdate as TzDate;
declare $c_tzdatetime as TzDatetime;
declare $c_tztimestamp as TzTimestamp;

declare $c_decimal0 as Decimal(20, 10);

declare $c_optional0 as Optional<string>;
declare $c_optional1 as string ?;
declare $c_optional2 as string??;
declare $c_optional3 as string? ? ?;
declare $c_optional4 as optional<string>?;

declare $c_tuple as Tuple< bool, uint64 >;
declare $c_struct as Struct< foo:string, 'bar':float >;
declare $c_variant1 as Variant< int, bool >;
declare $c_variant2 as Variant< foo:int, "bar":bool >;

declare $c_list0 as List<Yson>;
declare $c_list1 as List<$c_yson>;
declare $c_stream as Stream<Date>;
declare $c_flow as Flow<Uuid>;
declare $c_dict as Dict<string, interval>;
declare $c_set as Set<TzTimestamp>;
declare $c_enum as Enum<'first', 'second', 'third'>;

declare $c_resource0 as Resource<'foo'>;
declare $c_resource1 as Resource<bar>;
declare $resource as string;
declare $c_resource2 as Resource<$resource>;

declare $c_tagged as Tagged<Uint64, 'tag'>;
declare $c_generic as Generic;

declare $c_sometype0 as set<list<dict<string, int??>?>>;

declare $c_callable0 as Callable<()->bool>;
declare $c_callable1 as Callable<(int32)->int8>;
declare $c_callable2 as Callable<(int32, string)->float>;
declare $c_callable3 as Callable<(int32, foo : string)->float>;
declare $c_callable4 as Callable<(foo : int32, bar : string)->float>;
declare $c_callable5 as Callable<([foo : int32, bar : string])->float>;
declare $c_callable6 as Callable<(foo : int32, [bar : string])->float>;
declare $c_callable7 as Callable<(foo : int32, bar : optional<string>)->float>;
declare $c_callable8 as Callable<(int32{automap}, foo : string{Automap})->float>;

