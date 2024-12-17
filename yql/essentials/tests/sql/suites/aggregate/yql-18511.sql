/* yt can not */

$round_period = ($day, $period) -> {
    RETURN
    CASE
        WHEN $period = 'd' THEN $day
        WHEN $period = 'w' THEN DateTime::MakeDate(DateTime::StartOfWeek($day))
        WHEN $period = 'm' THEN DateTime::MakeDate(DateTime::StartOfMonth($day))
        ELSE $day
    END
};

$data = 
SELECT 
    $round_period(day, 'd') AS day,
    $round_period(day, 'w') AS week,
    $round_period(day, 'm') AS month,
    IF(user_card_cnt <= 10, user_card_cnt, 11) AS user_cards_segm,
    is_proven_owner,
    user_id,
FROM (
    SELECT 
        Date("2024-04-29") AS day,
        "ALLO" AS mark,
        "???" AS model,
        5 AS user_card_cnt,
        'ACTIVE' AS status,
        999 AS user_id,
        1 AS is_proven_owner,
    UNION ALL
    SELECT 
        Date("2024-04-29") AS day,
        "ALLO" AS mark,
        "!!!!!!" AS model,
        50 AS user_card_cnt,
        'ACTIVE' AS status,
        1111 AS user_id,
        0 AS is_proven_owner,
);

SELECT
    day,
    GROUPING(day) AS grouping_day,
    week,
    GROUPING(week) AS grouping_week,
    month,
    GROUPING(month) as grouping_month,
    CASE
        WHEN GROUPING(week) == 1 AND GROUPING(month) == 1 THEN 'd'
        WHEN GROUPING(day) == 1 AND GROUPING(month) == 1 THEN 'w'
        WHEN GROUPING(day) == 1 AND GROUPING(week) == 1 THEN 'm'
        ELSE NULL
    END AS period_type,
    user_cards_segm,
    if(GROUPING(user_cards_segm) = 1, -300, user_cards_segm) AS __user_cards_segm__,
    GROUPING(user_cards_segm) as grouping_user_cards_segm,
    COUNT(DISTINCT user_id) AS all_user_qty,
FROM $data AS t
GROUP BY
    GROUPING SETS(
        -- day grouping
        (day),
        (day, user_cards_segm),
        -- -- week grouping
        (week),
        (week, user_cards_segm),
        -- -- month grouping
        (month),
        (month, user_cards_segm)
    )
