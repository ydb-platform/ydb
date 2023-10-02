/* postgres can not */
select +Interval("P1D");
select -Interval("P1D");
select +Yql::Just(Interval("P1D"));
select -Yql::Just(Interval("P1D"));
select +Yql::Nothing(ParseType("Interval?"));
select -Yql::Nothing(ParseType("Interval?"));
