insert into plato.Output
select key, row_number() over w as r, session_start() over w as s from plato.Input
window w AS (PARTITION BY key, SessionWindow(cast(subkey as Datetime), DateTime::IntervalFromMinutes(15)));

