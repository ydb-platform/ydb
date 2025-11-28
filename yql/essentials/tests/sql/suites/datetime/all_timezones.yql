/* syntax version 1 */
$zones = CAST(ListMap(
    ListFromRange(0us, 1000us),
    ($i) -> { RETURN AsStruct($i AS id, SUBSTRING(CAST(YQL::AddTimezone(CurrentUtcDate(), $i) AS String), 11, NULL) AS zone) }
) AS List<Struct<id:Uint16, zone:Utf8>>);

SELECT * FROM AS_TABLE($zones);
