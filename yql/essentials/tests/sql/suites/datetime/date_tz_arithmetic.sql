/* postgres can not */
select cast(date("1970-01-02") - tzdate("1970-01-01,America/Los_Angeles") as string);
select cast(tzdate("1970-01-02,America/Los_Angeles") - date("1970-01-01") as string);
select cast(tzdate("1970-01-02,America/Los_Angeles") - tzdate("1970-01-01,America/Los_Angeles") as string);

select cast(tzdate("1970-01-01,America/Los_Angeles") + interval("P1D") as string);
select cast(interval("P1D") + tzdate("1970-01-01,America/Los_Angeles") as string);
select cast(tzdate("1970-01-02,America/Los_Angeles") - interval("P1D") as string);

select cast(datetime("1970-01-02T00:00:00Z") - tzdatetime("1970-01-01T00:00:00,America/Los_Angeles") as string);
select cast(tzdatetime("1970-01-02T00:00:00,America/Los_Angeles") - datetime("1970-01-01T00:00:00Z") as string);
select cast(tzdatetime("1970-01-02T00:00:00,America/Los_Angeles") - tzdatetime("1970-01-01T00:00:00,America/Los_Angeles") as string);

select cast(tzdatetime("1970-01-01T00:00:00,America/Los_Angeles") + interval("P1D") as string);
select cast(interval("P1D") + tzdatetime("1970-01-01T00:00:00,America/Los_Angeles") as string);
select cast(tzdatetime("1970-01-02T00:00:00,America/Los_Angeles") - interval("P1D") as string);

select cast(timestamp("1970-01-02T00:00:00.6Z") - tztimestamp("1970-01-01T00:00:00.3,America/Los_Angeles") as string);
select cast(tztimestamp("1970-01-02T00:00:00.6,America/Los_Angeles") - timestamp("1970-01-01T00:00:00.3Z") as string);
select cast(tztimestamp("1970-01-02T00:00:00.6,America/Los_Angeles") - tztimestamp("1970-01-01T00:00:00.3,America/Los_Angeles") as string);

select cast(tztimestamp("1970-01-01T00:00:00.6,America/Los_Angeles") + interval("P1D") as string);
select cast(interval("P1D") + tztimestamp("1970-01-01T00:00:00.6,America/Los_Angeles") as string);
select cast(tztimestamp("1970-01-02T00:00:00.6,America/Los_Angeles") - interval("P1D") as string);
