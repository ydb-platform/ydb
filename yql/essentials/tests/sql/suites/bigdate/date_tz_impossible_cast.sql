/* custom error:cannot be casted to TzDate32*/
SELECT
    CAST(AddTimezone(
        /* "-144169-01-01T21:29:42Z" */
        CAST(-4611669820218 AS DateTime64),
        "Europe/Moscow"
    ) AS TzDate32)
