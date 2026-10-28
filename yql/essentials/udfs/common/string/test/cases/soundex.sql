$input = AsList(
    <|key: "Miller"|>,
    <|key: "Ashcraft"|>,
    <|key: "Pfister"|>,
    <|key: "Tymczak"|>,
    <|key: "123"|>,
);

SELECT
    String::Soundex(key) AS soundex
FROM AS_TABLE($input);
