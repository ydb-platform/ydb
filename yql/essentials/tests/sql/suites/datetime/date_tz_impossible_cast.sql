SELECT
    CAST(AddTimezone(
        /* "1970-01-01T20:59:59Z" */
        CAST(75599 AS DateTime),
        "Europe/Moscow"
    ) AS TzDate)
