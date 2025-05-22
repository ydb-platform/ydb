{% include 'header.sql.jinja' %}

select  *
from (select avg(ss_list_price) B1_LP
            ,count(ss_list_price) B1_CNT
            ,count(distinct ss_list_price) B1_CNTD
      from {{store_sales}}
      where ss_quantity between 0 and 5
        and (ss_list_price between 8::numeric and (8+10)::numeric
             or ss_coupon_amt between 459::numeric and (459+1000)::numeric
             or ss_wholesale_cost between 57::numeric and (57+20)::numeric)) B1,
     (select avg(ss_list_price) B2_LP
            ,count(ss_list_price) B2_CNT
            ,count(distinct ss_list_price) B2_CNTD
      from {{store_sales}}
      where ss_quantity between 6 and 10
        and (ss_list_price between 90::numeric and (90+10)::numeric
          or ss_coupon_amt between 2323::numeric and (2323+1000)::numeric
          or ss_wholesale_cost between 31::numeric and (31+20)::numeric)) B2,
     (select avg(ss_list_price) B3_LP
            ,count(ss_list_price) B3_CNT
            ,count(distinct ss_list_price) B3_CNTD
      from {{store_sales}}
      where ss_quantity between 11 and 15
        and (ss_list_price between 142::numeric and (142+10)::numeric
          or ss_coupon_amt between 12214::numeric and (12214+1000)::numeric
          or ss_wholesale_cost between 79::numeric and (79+20)::numeric)) B3,
     (select avg(ss_list_price) B4_LP
            ,count(ss_list_price) B4_CNT
            ,count(distinct ss_list_price) B4_CNTD
      from {{store_sales}}
      where ss_quantity between 16 and 20
        and (ss_list_price between 135::numeric and (135+10)::numeric
          or ss_coupon_amt between 6071::numeric and (6071+1000)::numeric
          or ss_wholesale_cost between 38::numeric and (38+20)::numeric)) B4,
     (select avg(ss_list_price) B5_LP
            ,count(ss_list_price) B5_CNT
            ,count(distinct ss_list_price) B5_CNTD
      from {{store_sales}}
      where ss_quantity between 21 and 25
        and (ss_list_price between 122::numeric and (122+10)::numeric
          or ss_coupon_amt between 836::numeric and (836+1000)::numeric
          or ss_wholesale_cost between 17::numeric and (17+20)::numeric)) B5,
     (select avg(ss_list_price) B6_LP
            ,count(ss_list_price) B6_CNT
            ,count(distinct ss_list_price) B6_CNTD
      from {{store_sales}}
      where ss_quantity between 26 and 30
        and (ss_list_price between 154::numeric and (154+10)::numeric
          or ss_coupon_amt between 7326::numeric and (7326+1000)::numeric
          or ss_wholesale_cost between 7::numeric and (7+20)::numeric)) B6
limit 100;

-- end query 1 in stream 0 using template ../query_templates/query28.tpl
