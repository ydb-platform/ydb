/* postgres can not */
USE plato;
$list = AsList(
    AsList(3, 1),
    AsList(1, 1),
    AsList(1),
);

INSERT INTO @foo
SELECT
    x
FROM (
    SELECT
        $list AS x
)
    FLATTEN BY x;
COMMIT;

SELECT
    *
FROM @foo
ORDER BY
    x ASC;
