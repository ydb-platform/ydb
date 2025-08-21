/* postgres can not */
select AddTimezone(Datetime("2000-01-01T12:00:00Z"),"Europe/Moscow"),
   AddTimezone(Datetime("2000-01-01T12:00:00Z"),"America/Los_Angeles"),
   cast(RemoveTimezone(TzDatetime("2000-01-01T12:00:00,Europe/Moscow")) as string),
   cast(RemoveTimezone(TzDatetime("2000-01-01T12:00:00,America/Los_Angeles")) as string);
