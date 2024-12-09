/* postgres can not */
USE plato;

INSERT INTO @foo
SELECT
    *
FROM Input;
COMMIT;

$input = (
    SELECT
        *
    FROM Input
    WHERE key != "020"
    UNION ALL
    SELECT
        *
    FROM @foo
    UNION ALL
    SELECT
        *
    FROM Input
);

$output =
    SELECT
        key,
        ROW_NUMBER() OVER w AS row_num
    FROM $input
    WINDOW
        w AS ();

SELECT
    min(key) AS min_key,
    count(DISTINCT row_num) AS dist_rn,
    min(row_num) AS min_rn,
    max(row_num) AS max_rn,
FROM $output;
