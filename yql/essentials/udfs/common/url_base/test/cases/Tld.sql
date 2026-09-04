$input = AsList(
    <|value: "ru"|>,
    <|value: "123"|>,
    <|value: "yandex"|>,
    <|value: "sdfsdfsdf"|>,
);

SELECT
    Url::GetTLD(value) AS tld,
    Url::IsKnownTLD(value) AS known,
    Url::IsWellKnownTLD(value) AS well_known
FROM AS_TABLE($input);
