/* postgres can not */
select
    cast(Date("2000-01-01") as TzDate),
    cast(Datetime("2000-01-01T01:02:03Z") as TzDatetime),
    cast(Timestamp("2000-01-01T01:02:03.456789Z") as TzTimestamp);
