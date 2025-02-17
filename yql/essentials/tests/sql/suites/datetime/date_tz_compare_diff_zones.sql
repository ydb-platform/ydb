/* postgres can not */
select cast("2000-01-01,GMT" as tzdate) > tzdate("2000-01-01,Europe/Moscow");
select cast("1999-12-31,GMT" as tzdate) == tzdate("2000-01-01,Europe/Moscow");
select RemoveTimezone(cast("1999-12-31,GMT" as tzdate)) == RemoveTimezone(tzdate("2000-01-01,Europe/Moscow"));

select cast("2000-01-01,GMT" as tzdate) == tzdate("2000-01-01,America/Los_Angeles"); -- same time value
select RemoveTimezone(cast("2000-01-01,GMT" as tzdate)) == RemoveTimezone(tzdate("2000-01-01,America/Los_Angeles"));

select cast("2000-01-01T12:00:00,GMT" as tzdatetime) > tzdatetime("2000-01-01T12:00:00,Europe/Moscow");
select cast("2000-01-01T09:00:00,GMT" as tzdatetime) == tzdatetime("2000-01-01T12:00:00,Europe/Moscow");
select RemoveTimezone(cast("2000-01-01T09:00:00,GMT" as tzdatetime)) == RemoveTimezone(tzdatetime("2000-01-01T12:00:00,Europe/Moscow"));

select cast("2000-01-01T12:00:00,GMT" as tzdatetime) < tzdatetime("2000-01-01T12:00:00,America/Los_Angeles");
select cast("2000-01-01T20:00:00,GMT" as tzdatetime) == tzdatetime("2000-01-01T12:00:00,America/Los_Angeles");
select RemoveTimezone(cast("2000-01-01T20:00:00,GMT" as tzdatetime)) == RemoveTimezone(tzdatetime("2000-01-01T12:00:00,America/Los_Angeles"));

select cast("2000-01-01T12:00:00,GMT" as tztimestamp) > tztimestamp("2000-01-01T12:00:00,Europe/Moscow");
select cast("2000-01-01T09:00:00,GMT" as tztimestamp) == tztimestamp("2000-01-01T12:00:00,Europe/Moscow");
select RemoveTimezone(cast("2000-01-01T09:00:00,GMT" as tztimestamp)) == RemoveTimezone(tztimestamp("2000-01-01T12:00:00,Europe/Moscow"));

select cast("2000-01-01T12:00:00,GMT" as tztimestamp) < tztimestamp("2000-01-01T12:00:00,America/Los_Angeles");
select cast("2000-01-01T20:00:00,GMT" as tztimestamp) == tztimestamp("2000-01-01T12:00:00,America/Los_Angeles");
select RemoveTimezone(cast("2000-01-01T20:00:00,GMT" as tztimestamp)) == RemoveTimezone(tztimestamp("2000-01-01T12:00:00,America/Los_Angeles"));

