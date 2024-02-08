{% include 'header.sql.jinja' %}

select  *
from (select avg(ss_list_price) B1_LP
            ,count(ss_list_price) B1_CNT
            ,count(distinct ss_list_price) B1_CNTD
      from {{store_sales}}
      where ss_quantity between 0 and 5
        and (ss_list_price between 73::numeric and (73+10)::numeric
             or ss_coupon_amt between 7826::numeric and (7826+1000)::numeric
             or ss_wholesale_cost between 70::numeric and (70+20)::numeric)) B1,
     (select avg(ss_list_price) B2_LP
            ,count(ss_list_price) B2_CNT
            ,count(distinct ss_list_price) B2_CNTD
      from {{store_sales}}
      where ss_quantity between 6 and 10
        and (ss_list_price between 152::numeric and (152+10)::numeric
          or ss_coupon_amt between 2196::numeric and (2196+1000)::numeric
          or ss_wholesale_cost between 56::numeric and (56+20)::numeric)) B2,
     (select avg(ss_list_price) B3_LP
            ,count(ss_list_price) B3_CNT
            ,count(distinct ss_list_price) B3_CNTD
      from {{store_sales}}
      where ss_quantity between 11 and 15
        and (ss_list_price between 53::numeric and (53+10)::numeric
          or ss_coupon_amt between 3430::numeric and (3430+1000)::numeric
          or ss_wholesale_cost between 13::numeric and (13+20)::numeric)) B3,
     (select avg(ss_list_price) B4_LP
            ,count(ss_list_price) B4_CNT
            ,count(distinct ss_list_price) B4_CNTD
      from {{store_sales}}
      where ss_quantity between 16 and 20
        and (ss_list_price between 186::numeric and (186+10)::numeric
          or ss_coupon_amt between 3262::numeric and (3262+1000)::numeric
          or ss_wholesale_cost between 20::numeric and (20+20)::numeric)) B4,
     (select avg(ss_list_price) B5_LP
            ,count(ss_list_price) B5_CNT
            ,count(distinct ss_list_price) B5_CNTD
      from {{store_sales}}
      where ss_quantity between 21 and 25
        and (ss_list_price between 85::numeric and (85+10)::numeric
          or ss_coupon_amt between 3310::numeric and (3310+1000)::numeric
          or ss_wholesale_cost between 37::numeric and (37+20)::numeric)) B5,
     (select avg(ss_list_price) B6_LP
            ,count(ss_list_price) B6_CNT
            ,count(distinct ss_list_price) B6_CNTD
      from {{store_sales}}
      where ss_quantity between 26 and 30
        and (ss_list_price between 180::numeric and (180+10)::numeric
          or ss_coupon_amt between 12592::numeric and (12592+1000)::numeric
          or ss_wholesale_cost between 22::numeric and (22+20)::numeric)) B6
limit 100;

-- end query 1 in stream 0 using template ../query_templates/query28.tpl
