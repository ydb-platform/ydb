SELECT
    count(val) AS subkey,
    CAST(avg(val) AS int) AS value,
    value AS key
FROM (
    SELECT
        CASE key
            WHEN '0'
                THEN NULL
            ELSE CAST(subkey AS int) / CAST(key AS int)
        END AS val,
        value
    FROM plato.Input2
)
    AS res
GROUP BY
    value
ORDER BY
    value;
