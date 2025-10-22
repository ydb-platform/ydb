USE plato;

SELECT * FROM (
    SELECT
        value
    FROM RANGE("", "InputA", "InputC")
    WHERE
        (key == "023" OR key == "037") AND value != ""
    UNION ALL
    SELECT
        value
    FROM RANGE("", "InputA", "InputC")
    WHERE
        (key == "075" OR key == "150") AND value != ""
)
ORDER BY value;