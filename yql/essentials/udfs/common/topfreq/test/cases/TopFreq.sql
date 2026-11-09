$input = [
    <|key: "101", subkey: "1", value: "1"|>,
    <|key: "101", subkey: "2", value: "1"|>,
    <|key: "101", subkey: "3", value: "1"|>,
    <|key: "101", subkey: "4", value: "1"|>,
    <|key: "103", subkey: "1", value: "1"|>,
    <|key: "103", subkey: "2", value: "1"|>,
    <|key: "104", subkey: "3", value: "1"|>,
    <|key: "104", subkey: "4", value: "1"|>,
    <|key: "102", subkey: "1", value: "1"|>,
    <|key: "105", subkey: "2", value: "1"|>,
    <|key: "106", subkey: "3", value: "1"|>,
    <|key: "107", subkey: "4", value: "1"|>,
    <|key: "037", subkey: "5", value: "2"|>,
    <|key: "037", subkey: "6", value: "2"|>,
    <|key: "037", subkey: "7", value: "2"|>,
    <|key: "037", subkey: "8", value: "2"|>,
];

SELECT
    key, TopFreq(value, 3, 5u)
FROM
(SELECT
   key as value,
    "" as subkey,
    cast (value as Uint32) as key
FROM AS_TABLE($input))
AS tmp
GROUP BY key
ORDER BY key
