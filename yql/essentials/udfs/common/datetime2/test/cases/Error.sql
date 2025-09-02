SELECT
    DateTime::ToDays("DATE"),
    DateTime::TimeOfDay("TIME"),
    DateTime::TimeOfDay(Nothing(Optional<Resource<"INVALID">>));
