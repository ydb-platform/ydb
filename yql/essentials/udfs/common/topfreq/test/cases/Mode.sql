$input = [
    <|key: "075", subkey: "1", value: "abc"|>,
    <|key: "023", subkey: "2", value: "abc"|>,
    <|key: "023", subkey: "3", value: "abc"|>,
    <|key: "023", subkey: "4", value: "abc"|>,
    <|key: "037", subkey: "5", value: "bbb"|>,
    <|key: "037", subkey: "6", value: "bbb"|>,
    <|key: "200", subkey: "7", value: "bbb"|>,
    <|key: "037", subkey: "8", value: "bbb"|>,
];

SELECT
    key, Mode(value)
FROM
(SELECT
   cast (key as Int32) as value,
    "" as subkey,
    value as key
FROM AS_TABLE($input))
AS tmp
GROUP BY key
ORDER BY key
