/* syntax version 1 */
select 
    DateTime::Parse("%Y.%m.%d")("2016.08.15"),
    DateTime::Split(AddTimezone(DateTime("2017-01-01T10:00:00Z"),"Europe/Moscow"))
