{% include 'header.sql.jinja' %}

-- NB: Subquerys
$bla = (
         SELECT
         web_sales.ws_item_sk bla_item_sk,
            avg(ws_ext_discount_amt)  bla_ext_discount_amt
         FROM
            {{web_sales}} as web_sales
           cross join {{date_dim}} as date_dim
         WHERE
        cast(d_date as date) between cast('2001-03-12' as date) and
                             (cast('2001-03-12' as date) + DateTime::IntervalFromDays(90))
          and d_date_sk = ws_sold_date_sk
          group by web_sales.ws_item_sk
      );

-- start query 1 in stream 0 using template query92.tpl and seed 2031708268
select
   sum(ws_ext_discount_amt)  as `Excess Discount Amount`
from
    {{web_sales}} as web_sales
   cross join {{item}} as item
   cross join {{date_dim}} as date_dim
   join $bla bla on (item.i_item_sk = bla.bla_item_sk)
where
i_manufact_id = 356
and i_item_sk = ws_item_sk
and cast(d_date as date) between cast('2001-03-12' as date) and
        (cast('2001-03-12' as date) + DateTime::IntervalFromDays(90))
and d_date_sk = ws_sold_date_sk
and ws_ext_discount_amt
     > $z1_3 * bla.bla_ext_discount_amt
order by `Excess Discount Amount`
limit 100;

-- end query 1 in stream 0 using template query92.tpl
