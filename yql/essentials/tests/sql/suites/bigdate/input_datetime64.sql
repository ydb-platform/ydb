select cast("-144170-12-31T23:59:59Z" as timestamp64), cast(cast("-144170-12-31T23:59:59Z" as timestamp64) as string);
select cast("-144170-12-31T23:59:59-0:1" as timestamp64), cast(cast("-144170-12-31T23:59:59-0:1" as timestamp64) as string);
select cast("-144169-01-01T00:00:00+0:1" as timestamp64), cast(cast("-144169-01-01T00:00:00+0:1" as timestamp64) as string);
select cast("-144169-01-01T00:00:00-0:1" as timestamp64), cast(cast("-144169-01-01T00:00:00-0:1" as timestamp64) as string);
select cast("-144169-01-01T00:00:00Z" as timestamp64), cast(cast("-144169-01-01T00:00:00Z" as timestamp64) as string);

select cast("-1-1-1T00:00:00Z" as timestamp64), cast(cast("-1-1-1T00:00:00Z" as timestamp64) as string);
select cast("0-1-1T00:00:00Z" as timestamp64), cast(cast("0-1-1T00:00:00Z" as timestamp64) as string);
select cast("1-1-1T00:00:00Z" as timestamp64), cast(cast("1-1-1T00:00:00Z" as timestamp64) as string);
select cast("1-02-29T00:00:00Z" as timestamp64), cast(cast("1-02-29T00:00:00Z" as timestamp64) as string);

select cast("1969-12-31T00:00:00Z" as timestamp64), cast(cast("1969-12-31T00:00:00Z" as timestamp64) as string);
select cast("1969-12-31T23:59:59-0:1" as timestamp64), cast(cast("1969-12-31T23:59:59-0:1" as timestamp64) as string);
select cast("1970-01-01T00:00:00Z" as timestamp64), cast(cast("1970-01-01T00:00:00Z" as timestamp64) as string);
select cast("1970-01-01T00:00:00+0:1" as timestamp64), cast(cast("1970-01-01T00:00:00+0:1" as timestamp64) as string);

select cast("2000-04-05T00:00:00Z" as timestamp64), cast(cast("2000-04-05T00:00:00Z" as timestamp64) as string);
select cast("2100-02-29T00:00:00Z" as timestamp64), cast(cast("2100-02-29T00:00:00Z" as timestamp64) as string);
select cast("2100-03-01T00:00:00Z" as timestamp64), cast(cast("2100-03-01T00:00:00Z" as timestamp64) as string);
select cast("2105-12-31T00:00:00Z" as timestamp64), cast(cast("2105-12-31T00:00:00Z" as timestamp64) as string);
select cast("2106-01-01T00:00:00Z" as timestamp64), cast(cast("2106-01-01T00:00:00Z" as timestamp64) as string);

select cast("148107-12-31T23:59:59Z" as timestamp64), cast(cast("148107-12-31T23:59:59Z" as timestamp64) as string);
select cast("148107-12-31T23:59:59-0:1" as timestamp64), cast(cast("148107-12-31T23:59:59-0:1" as timestamp64) as string);
select cast("148107-12-31T23:59:59+0:1" as timestamp64), cast(cast("148107-12-31T23:59:59+0:1" as timestamp64) as string);
select cast("148108-01-01T00:00:00-0:1" as timestamp64), cast(cast("148108-01-01T00:00:00-0:1" as timestamp64) as string);
select cast("148108-01-01T00:00:00+0:1" as timestamp64), cast(cast("148108-01-01T00:00:00+0:1" as timestamp64) as string);
select cast("148108-01-01T00:00:00Z" as timestamp64), cast(cast("148108-01-01T00:00:00Z" as timestamp64) as string);
