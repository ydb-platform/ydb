/* syntax version 1 */
pragma warning("disable","4510");
SELECT CAST(DateTime::MakeDatetime64(DateTime::Update(
    CAST("2025-02-17T00:00:00Z" AS Datetime64), Yql::TimezoneId("Europe/Moscow") as TimezoneId)
) AS String);

SELECT CAST(DateTime::MakeDatetime64(DateTime::Update(
    CAST("2025-02-17T00:00:00Z" AS Datetime64), "Europe/Moscow" as Timezone)
) AS String);
