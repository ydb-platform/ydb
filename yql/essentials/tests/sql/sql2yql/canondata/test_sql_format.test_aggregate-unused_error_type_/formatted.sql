SELECT
    age
FROM (
    SELECT
        some(age) AS age,
        some(text) AS text
    FROM
        AS_TABLE([<|age: 1|>])
);
