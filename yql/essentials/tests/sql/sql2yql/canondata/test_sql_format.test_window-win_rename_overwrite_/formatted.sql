$src = [
    <|key: 1, subkey: 2, value: 3|>,
    <|key: 1, subkey: 3, value: 1|>,
    <|key: 1, subkey: 1, value: 2|>,
    <|key: 2, subkey: 20, value: 30|>,
    <|key: 2, subkey: 30, value: 10|>,
    <|key: 2, subkey: 10, value: 20|>,
];

SELECT
    key,
    subkey,
    row_number() OVER (
        PARTITION BY
            key
        ORDER BY
            subkey
    ) AS row,
    value
FROM (
    SELECT
        key AS key,
        subkey,
        row_number() OVER (
            PARTITION BY
                key
            ORDER BY
                subkey DESC
        ) AS row,
        min(value) OVER (
            PARTITION BY
                key
        ) AS minvalue,
        value
    FROM
        as_table($src)
);
