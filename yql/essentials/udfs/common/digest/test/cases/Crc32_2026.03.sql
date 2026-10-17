$input = AsList(
    <|key: "1"|>,
    <|key: "2"|>,
    <|key: "3"|>,
    <|key: ""|>,
);

SELECT
    Digest::Crc32(key) AS crc32
FROM AS_TABLE($input);
