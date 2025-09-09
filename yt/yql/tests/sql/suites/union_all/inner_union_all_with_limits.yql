USE plato;

SELECT
    key,
    value
FROM (
    (select * from Input limit 3)
    union all
    (select * from Input limit 2)
)
WHERE key < "100";

SELECT
    key,
    value
FROM (
    (select * from Input limit 3)
    union all
    select * from Input
)
WHERE key < "200";
