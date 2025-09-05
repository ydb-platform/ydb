--!syntax_pg
--TPC-DS Q28

-- start query 1 in stream 0 using template ../query_templates/query28.tpl
select  *
from (select avg(ss_list_price) B1_LP
            ,count(ss_list_price) B1_CNT
            ,count(distinct ss_list_price) B1_CNTD
      from plato.store_sales
      where ss_quantity between 0 and 5
        and (ss_list_price between 11::numeric and (11+10)::numeric
             or ss_coupon_amt between 460::numeric and (460+1000)::numeric
             or ss_wholesale_cost between 14::numeric and (14+20)::numeric)) B1,
     (select avg(ss_list_price) B2_LP
            ,count(ss_list_price) B2_CNT
            ,count(distinct ss_list_price) B2_CNTD
      from plato.store_sales
      where ss_quantity between 6 and 10
        and (ss_list_price between 91::numeric and (91+10)::numeric
          or ss_coupon_amt between 1430::numeric and (1430+1000)::numeric
          or ss_wholesale_cost between 32::numeric and (32+20)::numeric)) B2,
     (select avg(ss_list_price) B3_LP
            ,count(ss_list_price) B3_CNT
            ,count(distinct ss_list_price) B3_CNTD
      from plato.store_sales
      where ss_quantity between 11 and 15
        and (ss_list_price between 66::numeric and (66+10)::numeric
          or ss_coupon_amt between 920::numeric and (920+1000)::numeric
          or ss_wholesale_cost between 4::numeric and (4+20)::numeric)) B3,
     (select avg(ss_list_price) B4_LP
            ,count(ss_list_price) B4_CNT
            ,count(distinct ss_list_price) B4_CNTD
      from plato.store_sales
      where ss_quantity between 16 and 20
        and (ss_list_price between 142::numeric and (142+10)::numeric
          or ss_coupon_amt between 3054::numeric and (3054+1000)::numeric
          or ss_wholesale_cost between 80::numeric and (80+20)::numeric)) B4,
     (select avg(ss_list_price) B5_LP
            ,count(ss_list_price) B5_CNT
            ,count(distinct ss_list_price) B5_CNTD
      from plato.store_sales
      where ss_quantity between 21 and 25
        and (ss_list_price between 135::numeric and (135+10)::numeric
          or ss_coupon_amt between 14180::numeric and (14180+1000)::numeric
          or ss_wholesale_cost between 38::numeric and (38+20)::numeric)) B5,
     (select avg(ss_list_price) B6_LP
            ,count(ss_list_price) B6_CNT
            ,count(distinct ss_list_price) B6_CNTD
      from plato.store_sales
      where ss_quantity between 26 and 30
        and (ss_list_price between 28::numeric and (28+10)::numeric
          or ss_coupon_amt between 2513::numeric and (2513+1000)::numeric
          or ss_wholesale_cost between 42::numeric and (42+20)::numeric)) B6
limit 100;

-- end query 1 in stream 0 using template ../query_templates/query28.tpl
