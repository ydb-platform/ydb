{% include 'header.sql.jinja' %}

-- NB: Subquerys
$avg_discount_by_item = (
         select
            item.i_item_sk i_item_sk,
            avg(cs_ext_discount_amt) as avg_discout
         from
            {{catalog_sales}} as catalog_sales
           cross join {{date_dim}} as date_dim
           cross join {{item}} as item
         where cast (d_date as date) between cast('2002-03-29' as date) and
                             (cast('2002-03-29' as date) + DateTime::IntervalFromDays(90))
          and d_date_sk = cs_sold_date_sk
          and i_manufact_id = 66
          group by item.i_item_sk
      );

-- start query 1 in stream 0 using template query32.tpl and seed 2031708268
select  sum(cs_ext_discount_amt)  as `excess discount amount`
from
   {{catalog_sales}} cs
   cross join {{item}} as item
   cross join {{date_dim}} as date_dim
   join $avg_discount_by_item adi on cs.cs_item_sk = adi.i_item_sk
where
i_manufact_id = 66
and item.i_item_sk = cs.cs_item_sk
and cast (d_date as date) between cast('2002-03-29' as date) and
        (cast('2002-03-29' as date) + DateTime::IntervalFromDays(90))
and d_date_sk = cs_sold_date_sk
and cs_ext_discount_amt
     > $z1_3 * adi.avg_discout
limit 100;

-- end query 1 in stream 0 using template query32.tpl
