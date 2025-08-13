/* postgres can not */
select cast(date("1970-01-02") - date("1970-01-01") as string);

select cast(date("1970-01-01") + interval("P1D") as string);
select cast(interval("P1D") + date("1970-01-01") as string);
select cast(date("1970-01-02") - interval("P1D") as string);

select cast(datetime("1970-01-02T00:00:00Z") - datetime("1970-01-01T00:00:00Z") as string);

select cast(datetime("1970-01-01T00:00:00Z") + interval("P1D") as string);
select cast(interval("P1D") + datetime("1970-01-01T00:00:00Z") as string);
select cast(datetime("1970-01-02T00:00:00Z") - interval("P1D") as string);

select cast(timestamp("1970-01-02T00:00:00.6Z") - timestamp("1970-01-01T00:00:00.3Z") as string);

select cast(timestamp("1970-01-01T00:00:00.6Z") + interval("P1D") as string);
select cast(interval("P1D") + timestamp("1970-01-01T00:00:00.6Z") as string);
select cast(timestamp("1970-01-02T00:00:00.6Z") - interval("P1D") as string);

select cast(interval("P1D") + interval("P1D") as string);
select cast(interval("P1D") - interval("P1D") as string);

select cast(interval("P1D") * 2l as string);
select cast(2u * interval("P1D") as string);
select cast(interval("P1D") / 2 as string);
select cast(interval("P1D") / 0ut as string);
