PRAGMA YqlSelect = 'force';

SELECT
    person.id AS id,
    real_name,
    user_name,
    status,
FROM (
    SELECT
        *
    FROM (
        VALUES
            (1, 'ivan')
    ) AS person (
        id,
        real_name
    )
) AS person
JOIN (
    SELECT
        *
    FROM (
        VALUES
            (1, 'van')
    ) AS user (
        id,
        user_name
    )
) AS user
ON
    person.id == user.id
JOIN (
    SELECT
        *
    FROM (
        VALUES
            (1, 'ok')
    ) AS status (
        id,
        status
    )
) AS status
ON
    person.id == status.id
;
