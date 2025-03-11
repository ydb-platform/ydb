PRAGMA config.flags('OptimizerFlags', 'EqualityFilterOverJoin');
PRAGMA AnsiOptionalAs;

-- part of tpcds-6
$item = (
    SELECT
        *
    FROM
        as_table([
            <|i_current_price: Just(1.0f), i_category: Just('aaa'), i_item_sk: Just(125l)|>,
            <|i_current_price: Just(2.0f), i_category: Just('bbb'), i_item_sk: Just(999l)|>,
        ])
);

$sub2 = (
    SELECT
        i_current_price,
        i_category
    FROM
        $item
);

$customer_address = (
    SELECT
        *
    FROM
        as_table([
            <|ca_address_sk: Just(120l)|>,
            <|ca_address_sk: Just(150l)|>,
        ])
);

$customer = (
    SELECT
        *
    FROM
        as_table([
            <|c_current_addr_sk: Just(150l), c_customer_sk: Just(4l)|>,
            <|c_current_addr_sk: Just(120l), c_customer_sk: Just(2l)|>,
        ])
);

$store_sales = (
    SELECT
        *
    FROM
        as_table([
            <|ss_sold_date_sk: Just(1l), ss_customer_sk: Just(2l), ss_item_sk: Just(3l)|>,
            <|ss_sold_date_sk: Just(3l), ss_customer_sk: Just(4l), ss_item_sk: Just(5l)|>,
        ])
);

$date_dim = (
    SELECT
        *
    FROM
        as_table([
            <|d_date_sk: Just(1l)|>,
            <|d_date_sk: Just(2l)|>,
        ])
);

$item = (
    SELECT
        *
    FROM
        as_table([
            <|i_category: Just('aaa'), i_item_sk: Just(3l)|>,
            <|i_category: Just('bbb'), i_item_sk: Just(5l)|>,
        ])
);

SELECT
    JoinTableRow() cnt
FROM
    $customer_address a
CROSS JOIN
    $customer c
CROSS JOIN
    $store_sales s
CROSS JOIN
    $date_dim d
CROSS JOIN
    $item i
LEFT JOIN
    $sub2 AS j
ON
    i.i_category == j.i_category
WHERE
    s.ss_sold_date_sk == d.d_date_sk
    AND a.ca_address_sk == c.c_current_addr_sk
    AND c.c_customer_sk == s.ss_customer_sk
    AND s.ss_item_sk == i.i_item_sk
;
