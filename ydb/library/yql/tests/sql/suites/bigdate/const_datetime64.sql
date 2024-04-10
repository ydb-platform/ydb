select datetime64("-144169-01-01T00:00:00-0:1"), cast(datetime64("-144169-01-01T00:00:00-0:1") as string);
select datetime64("-144169-01-01T00:00:00Z"), cast(datetime64("-144169-01-01T00:00:00Z") as string);

select datetime64("-1-1-1T00:00:00Z"), cast(datetime64("-1-1-1T00:00:00Z") as string);
select datetime64("1-1-1T00:00:00Z"), cast(datetime64("1-1-1T00:00:00Z") as string);

select datetime64("1969-12-31T23:59:59Z"), cast(datetime64("1969-12-31T23:59:59Z") as string);
select datetime64("1969-12-31T23:59:59-0:1"), cast(datetime64("1969-12-31T23:59:59-0:1") as string);
select datetime64("1970-01-01T00:00:00Z"), cast(datetime64("1970-01-01T00:00:00Z") as string);
select datetime64("1970-1-1T0:0:1Z"), cast(datetime64("1970-1-1T0:0:1Z") as string);
select datetime64("1970-01-01T00:00:00+0:1"), cast(datetime64("1970-01-01T00:00:00+0:1") as string);

select datetime64("2000-04-05T00:00:00Z"), cast(datetime64("2000-04-05T00:00:00Z") as string);
select datetime64("2100-03-01T00:00:00Z"), cast(datetime64("2100-03-01T00:00:00Z") as string);
select datetime64("2105-12-31T00:00:00Z"), cast(datetime64("2105-12-31T00:00:00Z") as string);
select datetime64("2106-01-01T00:00:00Z"), cast(datetime64("2106-01-01T00:00:00Z") as string);

select datetime64("148107-12-31T23:59:59Z"), cast(datetime64("148107-12-31T23:59:59Z") as string);
select datetime64("148107-12-31T23:59:59+0:1"), cast(datetime64("148107-12-31T23:59:59+0:1") as string);
