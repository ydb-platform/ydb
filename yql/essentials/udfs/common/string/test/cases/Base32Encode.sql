$input = AsList(
    <|value:"test"|>,
    <|value:"TestTest"|>,
    <|value:"apple"|>
);

SELECT
    value,
    String::Base32Encode(value) AS encoded
FROM AS_TABLE($input);
