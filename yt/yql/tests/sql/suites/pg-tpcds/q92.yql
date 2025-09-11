--!syntax_pg
--TPC-DS Q92

-- start query 1 in stream 0 using template ../query_templates/query92.tpl
select  
   sum(ws_ext_discount_amt)  as "Excess Discount Amount" 
from 
    plato.web_sales 
   ,plato.item 
   ,plato.date_dim
where
i_manufact_id = 269
and i_item_sk = ws_item_sk 
and d_date between '1998-03-18'::date and 
        (cast('1998-03-18' as date) + interval '90' day)::date
and d_date_sk = ws_sold_date_sk 
and ws_ext_discount_amt  
     > ( 
         SELECT 
            1.3::numeric * avg(ws_ext_discount_amt) 
         FROM 
            plato.web_sales 
           ,plato.date_dim
         WHERE 
              ws_item_sk = i_item_sk 
          and d_date between '1998-03-18'::date and
                             (cast('1998-03-18' as date) + interval '90' day)::date
          and d_date_sk = ws_sold_date_sk 
      ) 
order by sum(ws_ext_discount_amt)
limit 100;

-- end query 1 in stream 0 using template ../query_templates/query92.tpl
