$input = AsList(
    <|key: "1"|>,
    <|key: "2"|>,
    <|key: "3"|>,
    <|key: ""|>,
);

SELECT
    Digest::Sha224(key) AS sha224
FROM AS_TABLE($input);
