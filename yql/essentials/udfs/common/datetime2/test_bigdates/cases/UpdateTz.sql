pragma warning("disable","4510");
SELECT CAST(DateTime::MakeDatetime64(DateTime::Update(
    Just(Datetime64("2025-02-17T00:00:00Z")), Yql::TimezoneId("Europe/Moscow") as TimezoneId)
) AS String);

SELECT CAST(DateTime::MakeDatetime64(DateTime::Update(
    Just(Datetime64("2025-02-17T00:00:00Z")), "Europe/Moscow" as Timezone)
) AS String);
