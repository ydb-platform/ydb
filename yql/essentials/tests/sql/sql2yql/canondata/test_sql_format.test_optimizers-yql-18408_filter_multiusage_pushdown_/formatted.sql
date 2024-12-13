USE plato;

PRAGMA AnsiOptionalAs;
PRAGMA config.flags('OptimizerFlags', 'FieldSubsetEnableMultiusage', 'FilterPushdownEnableMultiusage', 'EarlyExpandSkipNull');

$date_dim =
    SELECT
        *
    FROM
        as_table([
            <|d_year: Just(1999), d_date_sk: Just(10001)|>,
            <|d_year: Just(1999), d_date_sk: Just(10002)|>,
            <|d_year: Just(1999), d_date_sk: Just(10003)|>,
            <|d_year: Just(2000), d_date_sk: Just(10001)|>,
            <|d_year: Just(2000), d_date_sk: Just(10002)|>,
            <|d_year: Just(2000), d_date_sk: Just(10003)|>,
        ])
;

$customer =
    SELECT
        *
    FROM
        as_table([
            <|c_customer_sk: Just(1), c_customer_id: Just(1), c_first_name: Just('Vasya'), c_last_name: Just('Ivanov'), c_preferred_cust_flag: Just('aaa'), c_birth_country: Just('RU'), c_login: Just('ivanov'), c_email_address: Just('foo@bar.com')|>,
            <|c_customer_sk: Just(2), c_customer_id: Just(2), c_first_name: Just('Petya'), c_last_name: Just('Ivanov'), c_preferred_cust_flag: Just('bbb'), c_birth_country: Just('RU'), c_login: Just('ivanov1'), c_email_address: Just('foo1@bar.com')|>,
            <|c_customer_sk: Just(3), c_customer_id: NULL, c_first_name: NULL, c_last_name: NULL, c_preferred_cust_flag: NULL, c_birth_country: NULL, c_login: Just('ivanov1'), c_email_address: Just('foo2@bar.com')|>,
        ])
;

$store_sales =
    SELECT
        *
    FROM
        as_table([
            <|ss_sold_date_sk: Just(10001), ss_customer_sk: Just(1), ss_ext_list_price: Just(12345), ss_ext_discount_amt: Just(1234)|>,
            <|ss_sold_date_sk: Just(10002), ss_customer_sk: Just(2), ss_ext_list_price: Just(12346), ss_ext_discount_amt: Just(123)|>,
            <|ss_sold_date_sk: Just(10003), ss_customer_sk: Just(3), ss_ext_list_price: Just(12347), ss_ext_discount_amt: Just(1235)|>,
        ])
;

INSERT INTO @date_dim
SELECT
    *
FROM
    $date_dim
;

INSERT INTO @customer
SELECT
    *
FROM
    $customer
;

INSERT INTO @store_sales
SELECT
    *
FROM
    $store_sales
;

COMMIT;

$year_total = (
    SELECT
        customer.c_customer_id customer_id,
        customer.c_first_name customer_first_name,
        customer.c_last_name customer_last_name,
        customer.c_preferred_cust_flag customer_preferred_cust_flag,
        customer.c_birth_country customer_birth_country,
        customer.c_login customer_login,
        customer.c_email_address customer_email_address,
        date_dim.d_year dyear,
        sum(ss_ext_list_price - ss_ext_discount_amt) year_total,
        's' sale_type
    FROM
        @date_dim date_dim
    CROSS JOIN
        @store_sales store_sales
    CROSS JOIN
        @customer customer
    WHERE
        ss_sold_date_sk == d_date_sk AND c_customer_sk == ss_customer_sk
    GROUP BY
        customer.c_customer_id,
        customer.c_first_name,
        customer.c_last_name,
        customer.c_preferred_cust_flag,
        customer.c_birth_country,
        customer.c_login,
        customer.c_email_address,
        date_dim.d_year
);

SELECT
    t_s_secyear.customer_id,
    t_s_secyear.customer_first_name,
    t_s_secyear.customer_last_name,
    t_s_secyear.customer_birth_country
FROM
    $year_total t_s_firstyear
CROSS JOIN
    $year_total t_s_secyear
WHERE
    t_s_secyear.customer_id == t_s_firstyear.customer_id
    AND t_s_firstyear.sale_type == 's'
    AND t_s_secyear.sale_type == 's'
    AND t_s_firstyear.dyear == 1999
    AND t_s_secyear.dyear == 1999 + 1
ORDER BY
    t_s_secyear.customer_id,
    t_s_secyear.customer_first_name,
    t_s_secyear.customer_last_name,
    t_s_secyear.customer_birth_country
LIMIT 100;
