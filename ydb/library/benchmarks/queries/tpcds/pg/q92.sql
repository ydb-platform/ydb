{% include 'header.sql.jinja' %}

select
   sum(ws_ext_discount_amt)  as "Excess Discount Amount"
from
    {{web_sales}}
   ,{{item}}
   ,{{date_dim}}
where
i_manufact_id = 356
and i_item_sk = ws_item_sk
and d_date between '2001-03-12'::date and
        (cast('2001-03-12' as date) + interval '90' day)::date
and d_date_sk = ws_sold_date_sk
and ws_ext_discount_amt
     > (
         SELECT
            1.3::numeric * avg(ws_ext_discount_amt)
         FROM
            {{web_sales}}
           ,{{date_dim}}
         WHERE
              ws_item_sk = i_item_sk
          and d_date between '2001-03-12'::date and
                             (cast('2001-03-12' as date) + interval '90' day)::date
          and d_date_sk = ws_sold_date_sk
      )
order by sum(ws_ext_discount_amt)
limit 100;

-- end query 1 in stream 0 using template ../query_templates/query92.tpl
