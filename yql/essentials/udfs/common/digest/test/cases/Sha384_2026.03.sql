$input = AsList(
    <|key: "1"|>,
    <|key: "2"|>,
    <|key: "3"|>,
    <|key: ""|>,
);

SELECT
    Digest::Sha384(key) AS sha384
FROM AS_TABLE($input);
