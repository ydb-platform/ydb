--
-- with microseconds
--

select timestamp64("-144169-01-01T00:00:00.000000-0:1"), cast(timestamp64("-144169-01-01T00:00:00.000000-0:1") as string);
select timestamp64("-144169-01-01T00:00:00.000000Z"), cast(timestamp64("-144169-01-01T00:00:00.000000Z") as string);

select timestamp64("1969-12-31T23:59:59.999999Z"), cast(timestamp64("1969-12-31T23:59:59.999999Z") as string);
select timestamp64("1969-12-31T23:59:59.999999-0:1"), cast(timestamp64("1969-12-31T23:59:59.999999-0:1") as string);
select timestamp64("1970-1-1T0:0:0.0Z"), cast(timestamp64("1970-1-1T0:0:0.0Z") as string);
select timestamp64("1970-01-01T00:00:00.000001Z"), cast(timestamp64("1970-01-01T00:00:00.000001Z") as string);
select timestamp64("1970-01-01T00:00:00.000001+0:1"), cast(timestamp64("1970-01-01T00:00:00.000001+0:1") as string);

select timestamp64("148107-12-31T23:59:59.999999Z"), cast(timestamp64("148107-12-31T23:59:59.999999Z") as string);
select timestamp64("148107-12-31T23:59:59.999999+0:1"), cast(timestamp64("148107-12-31T23:59:59.999999+0:1") as string);

--
-- without microseconds (like in datetime64)
--

select timestamp64("-144169-01-01T00:00:00-0:1"), cast(timestamp64("-144169-01-01T00:00:00-0:1") as string);
select timestamp64("-144169-01-01T00:00:00Z"), cast(timestamp64("-144169-01-01T00:00:00Z") as string);

select timestamp64("-1-1-1T00:00:00Z"), cast(timestamp64("-1-1-1T00:00:00Z") as string);
select timestamp64("1-1-1T00:00:00Z"), cast(timestamp64("1-1-1T00:00:00Z") as string);

select timestamp64("1969-12-31T00:00:00Z"), cast(timestamp64("1969-12-31T00:00:00Z") as string);
select timestamp64("1969-12-31T23:59:59-0:1"), cast(timestamp64("1969-12-31T23:59:59-0:1") as string);
select timestamp64("1970-01-01T00:00:00Z"), cast(timestamp64("1970-01-01T00:00:00Z") as string);
select timestamp64("1970-01-01T00:00:00+0:1"), cast(timestamp64("1970-01-01T00:00:00+0:1") as string);

select timestamp64("2000-04-05T00:00:00Z"), cast(timestamp64("2000-04-05T00:00:00Z") as string);
select timestamp64("2100-03-01T00:00:00Z"), cast(timestamp64("2100-03-01T00:00:00Z") as string);
select timestamp64("2105-12-31T00:00:00Z"), cast(timestamp64("2105-12-31T00:00:00Z") as string);
select timestamp64("2106-01-01T00:00:00Z"), cast(timestamp64("2106-01-01T00:00:00Z") as string);

select timestamp64("148107-12-31T23:59:59Z"), cast(timestamp64("148107-12-31T23:59:59Z") as string);
select timestamp64("148107-12-31T23:59:59+0:1"), cast(timestamp64("148107-12-31T23:59:59+0:1") as string);

