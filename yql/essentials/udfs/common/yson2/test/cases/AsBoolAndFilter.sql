$data = [
    <|verdict1: '{k=%true}'y, verdict2: '{k=%false}'y|>,
    <|verdict1: '{k=%false}'y, verdict2: '{k=%false}'y|>,
    <|verdict1: '{k=%true}'y, verdict2: '{k=%true}'y|>,
];

SELECT
    Yson::AsBool(verdict1.k) AS verdict1_val,
    Yson::AsBool(verdict2.k) AS verdict2_val
FROM AS_TABLE($data)
WHERE NOT Yson::AsBool(verdict2.k)
  AND Yson::AsBool(verdict1.k);
