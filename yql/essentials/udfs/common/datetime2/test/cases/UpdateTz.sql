/* syntax version 1 */
pragma warning("disable","4510");
select cast(DateTime::MakeDatetime(
    DateTime::Update(Datetime("2000-01-01T00:00:00Z"), Yql::TimezoneId("Europe/Moscow") as TimezoneId)
) as string);

select cast(DateTime::MakeDatetime(
    DateTime::Update(Datetime("2000-01-01T00:00:00Z"), "Europe/Moscow" as Timezone)
) as string);
