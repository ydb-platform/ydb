PRAGMA AnsiInForEmptyOrNullableItemsCollections;

$prepared = (
    SELECT
        tm.date_window AS date_window,
        Substring(CAST(tm.date_window AS String), 0, 7) AS ym, -- YYYY-MM
        tm.branch AS branch,
        tm.build_type AS build_type,
        tm.owner AS owner,
        tm.full_name AS full_name
    FROM `test_results/analytics/tests_monitor` AS tm
    WHERE tm.date_window >= CurrentUtcDate() - 365 * Interval("P1D")
      AND (tm.branch = 'main' OR tm.branch LIKE 'stable-%' OR tm.branch LIKE 'stream-nb-25%')
      AND tm.is_test_chunk = 0
      AND tm.is_muted = 1
);

-- Дневная метрика
$daily = (
    SELECT
        date_window,
        ym,
        branch,
        build_type,
        owner,
        COUNT(DISTINCT full_name) AS daily_value
    FROM $prepared
    GROUP BY
        date_window,
        ym,
        branch,
        build_type,
        owner
);

-- Последняя дата в каждом месяце по группе
$month_last_day = (
    SELECT
        ym,
        branch,
        build_type,
        owner,
        MAX(date_window) AS last_day_in_month
    FROM $daily
    GROUP BY
        ym,
        branch,
        build_type,
        owner
);

-- Значение метрики на последнюю дату месяца
$month_value = (
    SELECT
        m.ym AS ym,
        m.branch AS branch,
        m.build_type AS build_type,
        m.owner AS owner,
        m.last_day_in_month AS month_point_date,
        d.daily_value AS month_value
    FROM $month_last_day AS m
    INNER JOIN $daily AS d
        ON d.ym = m.ym
        AND d.branch = m.branch
        AND d.build_type = m.build_type
        AND d.owner = m.owner
        AND d.date_window = m.last_day_in_month
);

$ranked = (
    SELECT
        ym,
        branch,
        build_type,
        owner,
        month_point_date,
        month_value,
        LAG(month_value) OVER (
            PARTITION BY branch, build_type, owner
            ORDER BY ym
        ) AS prev_month_value
    FROM $month_value
);

SELECT
    ym,
    branch,
    build_type,
    owner,
    month_point_date,
    month_value AS count_of_muted_tests,
    prev_month_value,
    CASE
        WHEN prev_month_value IS NOT NULL
            THEN CAST(month_value AS Int64) - CAST(prev_month_value AS Int64)
        ELSE NULL
    END AS delta_vs_prev_month_end
FROM $ranked;
