use plato;

$min_ts_for_stat_calculation = DateTime::ToSeconds(CurrentUtcDate() - Interval("P1D"));

insert into @a
select * from (
    select 1ul as puid, CurrentUtcTimestamp() as timestamp, [1, 2] as segments, "a" as dummy1
)
assume order by puid, timestamp desc;

insert into @b
select * from (
    select 4ul as puid, CurrentUtcTimestamp() as timestamp, [3, 2] as segments, "a" as dummy1
)
assume order by puid, timestamp desc;

insert into @c
select * from (
    select 2ul as puid, Just(CurrentUtcTimestamp()) as timestamp, [2, 3] as segments, "a" as dummy2
)
assume order by puid, timestamp desc;

commit;

$target_events = (
    SELECT
        puid,
        segments
    FROM CONCAT(@a, @b, @c)
    where DateTime::ToSeconds(`timestamp`) > $min_ts_for_stat_calculation
);

$target_events = (
    SELECT DISTINCT *
    FROM (
        SELECT *
        FROM $target_events
        FLATTEN LIST BY segments
    )
    FLATTEN COLUMNS
);

SELECT * FROM $target_events ORDER BY puid, segments;
