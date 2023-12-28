{% include 'header.sql.jinja' %}

$ssci = (
    SELECT ss.ss_customer_sk as customer_sk
          ,ss.ss_item_sk as item_sk
    FROM {{store_sales}} as ss
    CROSS JOIN {{date_dim}} as dd
    WHERE ss.ss_sold_date_sk = dd.d_date_sk
          AND dd.d_month_seq BETWEEN 1190 AND 1190 + 11
    GROUP BY ss.ss_customer_sk, ss.ss_item_sk
);

$csci = (
    SELECT cs.cs_bill_customer_sk as customer_sk
          ,cs.cs_item_sk as item_sk
    FROM {{catalog_sales}} as cs
    CROSS JOIN {{date_dim}} as dd
    WHERE cs.cs_sold_date_sk = dd.d_date_sk
        AND dd.d_month_seq BETWEEN 1190 AND 1190 + 11
    GROUP BY cs.cs_bill_customer_sk
            ,cs.cs_item_sk
);

SELECT SUM(case when ssci.customer_sk is not null and csci.customer_sk is null then 1 else 0 end) as store_only
       ,SUM(case when ssci.customer_sk is null and csci.customer_sk is not null then 1 else 0 end) as catalog_only
       ,SUM(case when ssci.customer_sk is not null and csci.customer_sk is not null then 1 else 0 end) as store_and_catalog
FROM $ssci as ssci
FULL JOIN $csci as csci ON (ssci.customer_sk = csci.customer_sk
                            AND ssci.item_sk = csci.item_sk)
LIMIT 100;
