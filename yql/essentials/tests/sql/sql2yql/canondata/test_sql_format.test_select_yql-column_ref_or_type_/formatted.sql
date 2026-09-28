PRAGMA YqlSelect = 'force';

SELECT
    EvaluateExpr(FormatType(TypeOf(AsErased(Int64)))) AS type_without_from
;

SELECT
    Unwrap(PeekErased(AsErased(Int32), Int32)) AS column_wins,
    PeekErased(AsErased(42), Int64) AS type_with_ambiguous_columns,
    (
        SELECT
            Unwrap(PeekErased(AsErased(CAST(1 AS Int64)), Int64))
        FROM (
            VALUES
                (0)
        ) AS inner_row (
            key
        )
    ) AS scalar_from_uncorrelated,
    (
        SELECT
            Unwrap(PeekErased(AsErased(lhs.Int64), Int64))
        FROM (
            VALUES
                (0)
        ) AS inner_row (
            key
        )
    ) AS scalar_from_correlated_qualified,
    (
        SELECT
            Unwrap(PeekErased(AsErased(CAST(3 AS Int64)), Int64))
        FROM (
            VALUES
                (0)
        ) AS inner_lhs (
            key
        )
        JOIN (
            VALUES
                (0)
        ) AS inner_rhs (
            key
        )
        ON
            inner_lhs.key == inner_rhs.key
    ) AS scalar_join_uncorrelated,
    (
        SELECT
            Unwrap(PeekErased(AsErased(lhs.Int64), Int64))
        FROM (
            VALUES
                (0)
        ) AS inner_lhs (
            key
        )
        JOIN (
            VALUES
                (0)
        ) AS inner_rhs (
            key
        )
        ON
            inner_lhs.key == inner_rhs.key
    ) AS scalar_join_correlated_qualified
FROM (
    VALUES
        (42, CAST(1 AS Int64))
) AS lhs (
    Int32,
    Int64
)
CROSS JOIN (
    VALUES
        (CAST(2 AS Int64))
) AS rhs (
    Int64
);

SELECT
    src.Int64 AS in_and_exists_subqueries_resolve_types
FROM (
    VALUES
        (CAST(7 AS Int64))
) AS src (
    Int64
)
WHERE
    src.Int64 IN (
        SELECT
            Unwrap(PeekErased(AsErased(CAST(7 AS Int64)), Int64))
        FROM (
            VALUES
                (0)
        ) AS inner_row (
            key
        )
    )
    AND src.Int64 IN (
        SELECT
            Unwrap(PeekErased(AsErased(src.Int64), Int64))
        FROM (
            VALUES
                (0)
        ) AS inner_row (
            key
        )
    )
    AND EXISTS (
        SELECT
            1
        FROM (
            VALUES
                (0)
        ) AS inner_row (
            key
        )
        WHERE
            Unwrap(PeekErased(AsErased(CAST(6 AS Int64)), Int64)) == 6
    )
    AND EXISTS (
        SELECT
            1
        FROM (
            VALUES
                (0)
        ) AS inner_row (
            key
        )
        WHERE
            Unwrap(PeekErased(AsErased(src.Int64), Int64)) == src.Int64
    )
;

PRAGMA YqlSelect = 'auto';

$legacy = (
    SELECT
        *
    FROM
        AsTable([
            <|Int64: [CAST(8 AS Int64)]|>
        ])
        FLATTEN BY Int64
);

SELECT
    Unwrap(PeekErased(AsErased(Int64), Int64)) AS auto_with_legacy_source
FROM
    $legacy
;
