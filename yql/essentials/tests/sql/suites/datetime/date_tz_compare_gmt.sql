/* postgres can not */
select cast("2000-01-01,GMT" as tzdate) == cast("2000-01-01" as date);
select cast("2000-01-01,GMT" as tzdate) < cast("2000-01-01" as date);
select cast("2000-01-01,GMT" as tzdate) <= cast("2000-01-01" as date);
select cast("2000-01-01,GMT" as tzdate) > cast("2000-01-01" as date);
select cast("2000-01-01,GMT" as tzdate) >= cast("2000-01-01" as date);
select cast("2000-01-01,GMT" as tzdate) != cast("2000-01-01" as date);

select cast("2000-01-01" as date) == cast("2000-01-01,GMT" as tzdate);
select cast("2000-01-01" as date) < cast("2000-01-01,GMT" as tzdate);
select cast("2000-01-01" as date) <= cast("2000-01-01,GMT" as tzdate);
select cast("2000-01-01" as date) > cast("2000-01-01,GMT" as tzdate);
select cast("2000-01-01" as date) >= cast("2000-01-01,GMT" as tzdate);
select cast("2000-01-01" as date) != cast("2000-01-01,GMT" as tzdate);

select cast("2000-01-01T12:00:00,GMT" as tzdatetime) == cast("2000-01-01T12:00:00Z" as datetime);
select cast("2000-01-01T12:00:00,GMT" as tzdatetime) < cast("2000-01-01T12:00:00Z" as datetime);
select cast("2000-01-01T12:00:00,GMT" as tzdatetime) <= cast("2000-01-01T12:00:00Z" as datetime);
select cast("2000-01-01T12:00:00,GMT" as tzdatetime) > cast("2000-01-01T12:00:00Z" as datetime);
select cast("2000-01-01T12:00:00,GMT" as tzdatetime) >= cast("2000-01-01T12:00:00Z" as datetime);
select cast("2000-01-01T12:00:00,GMT" as tzdatetime) != cast("2000-01-01T12:00:00Z" as datetime);

select cast("2000-01-01T12:00:00Z" as datetime) == cast("2000-01-01T12:00:00,GMT" as tzdatetime);
select cast("2000-01-01T12:00:00Z" as datetime) < cast("2000-01-01T12:00:00,GMT" as tzdatetime);
select cast("2000-01-01T12:00:00Z" as datetime) <= cast("2000-01-01T12:00:00,GMT" as tzdatetime);
select cast("2000-01-01T12:00:00Z" as datetime) > cast("2000-01-01T12:00:00,GMT" as tzdatetime);
select cast("2000-01-01T12:00:00Z" as datetime) >= cast("2000-01-01T12:00:00,GMT" as tzdatetime);
select cast("2000-01-01T12:00:00Z" as datetime) != cast("2000-01-01T12:00:00,GMT" as tzdatetime);

select cast("2000-01-01T12:00:00.123456,GMT" as tztimestamp) == cast("2000-01-01T12:00:00.123456Z" as timestamp);
select cast("2000-01-01T12:00:00.123456,GMT" as tztimestamp) < cast("2000-01-01T12:00:00.123456Z" as timestamp);
select cast("2000-01-01T12:00:00.123456,GMT" as tztimestamp) <= cast("2000-01-01T12:00:00.123456Z" as timestamp);
select cast("2000-01-01T12:00:00.123456,GMT" as tztimestamp) > cast("2000-01-01T12:00:00.123456Z" as timestamp);
select cast("2000-01-01T12:00:00.123456,GMT" as tztimestamp) >= cast("2000-01-01T12:00:00.123456Z" as timestamp);
select cast("2000-01-01T12:00:00.123456,GMT" as tztimestamp) != cast("2000-01-01T12:00:00.123456Z" as timestamp);

select cast("2000-01-01T12:00:00.123456Z" as timestamp) == cast("2000-01-01T12:00:00.123456,GMT" as tztimestamp);
select cast("2000-01-01T12:00:00.123456Z" as timestamp) < cast("2000-01-01T12:00:00.123456,GMT" as tztimestamp);
select cast("2000-01-01T12:00:00.123456Z" as timestamp) <= cast("2000-01-01T12:00:00.123456,GMT" as tztimestamp);
select cast("2000-01-01T12:00:00.123456Z" as timestamp) > cast("2000-01-01T12:00:00.123456,GMT" as tztimestamp);
select cast("2000-01-01T12:00:00.123456Z" as timestamp) >= cast("2000-01-01T12:00:00.123456,GMT" as tztimestamp);
select cast("2000-01-01T12:00:00.123456Z" as timestamp) != cast("2000-01-01T12:00:00.123456,GMT" as tztimestamp);
