$input = AsList(
    <|key: "1"|>,
    <|key: "2"|>,
    <|key: "3"|>,
    <|key: ""|>,
);

SELECT
    Digest::Sha512(key) AS sha512
FROM AS_TABLE($input);
