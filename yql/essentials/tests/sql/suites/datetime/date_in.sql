/* postgres can not */
select cast("1970-01-01" as date),cast(cast("1970-01-01" as date) as string);
select cast("2000-04-05" as date),cast(cast("2000-04-05" as date) as string);
select cast("2100-02-29" as date),cast(cast("2100-02-29" as date) as string);
select cast("2100-03-01" as date),cast(cast("2100-03-01" as date) as string);

select cast("2000-04-05T06:07:08Z" as datetime),cast(cast("2000-04-05T06:07:08Z" as datetime) as string);
select cast("2000-04-05T06:07:08Z" as timestamp),cast(cast("2000-04-05T06:07:08Z" as timestamp) as string);
select cast("2000-04-05T06:07:08.9Z" as timestamp),cast(cast("2000-04-05T06:07:08.9Z" as timestamp) as string);
select cast("2000-04-05T06:07:08.000009Z" as timestamp),cast(cast("2000-04-05T06:07:08.000009Z" as timestamp) as string);

select cast("P" as interval),cast(cast("P" as interval) as string);
select cast("P1D" as interval),cast(cast("P1D" as interval) as string);
select cast("-P1D" as interval),cast(cast("-P1D" as interval) as string);
select cast("PT2H" as interval),cast(cast("PT2H" as interval) as string);
select cast("PT2H3M" as interval),cast(cast("PT2H3M" as interval) as string);
select cast("PT3M" as interval),cast(cast("PT3M" as interval) as string);
select cast("PT3M4S" as interval),cast(cast("PT3M4S" as interval) as string);
select cast("PT4S" as interval),cast(cast("PT4S" as interval) as string);
select cast("PT0S" as interval),cast(cast("PT0S" as interval) as string);
select cast("PT4.5S" as interval),cast(cast("PT4.5S" as interval) as string);
select cast("PT4.000005S" as interval),cast(cast("PT4.000005S" as interval) as string);
select cast("P1DT2H3M4.5S" as interval),cast(cast("P1DT2H3M4.5S" as interval) as string);

select cast("2105-12-31" as date),cast(cast("2105-12-31" as date) as string);
select cast("2106-01-01" as date),cast(cast("2106-01-01" as date) as string);

select cast("2105-12-31T23:59:59Z" as datetime),cast(cast("2105-12-31T23:59:59Z" as datetime) as string);
select cast("2106-01-01T00:00:00Z" as datetime),cast(cast("2106-01-01T00:00:00Z" as datetime) as string);

select cast("2105-12-31T23:59:59.999999Z" as timestamp),cast(cast("2105-12-31T23:59:59.999999Z" as timestamp) as string);
select cast("2106-01-01T00:00:00.000000Z" as timestamp),cast(cast("2106-01-01T00:00:00.000000Z" as timestamp) as string);

select cast("P49672DT23H59M59.999999S" as interval),cast(cast("P49672DT23H59M59.999999S" as interval) as string);
select cast("P49673D" as interval),cast(cast("P49673D" as interval) as string);
