--!syntax_pg
--TPC-DS Q32

-- start query 1 in stream 0 using template ../query_templates/query32.tpl
select  sum(cs_ext_discount_amt)  as "excess discount amount" 
from 
   plato.catalog_sales 
   ,plato.item 
   ,plato.date_dim
where
i_manufact_id = 269
and i_item_sk = cs_item_sk 
and d_date between '1998-03-18'::date and 
        (cast('1998-03-18' as date) + interval '90' day)::date
and d_date_sk = cs_sold_date_sk 
and cs_ext_discount_amt  
     > ( 
         select 
            1.3::numeric * avg(cs_ext_discount_amt) 
         from 
            plato.catalog_sales 
           ,plato.date_dim
         where 
              cs_item_sk = i_item_sk 
          and d_date between '1998-03-18'::date and
                             (cast('1998-03-18' as date) + interval '90' day)::date
          and d_date_sk = cs_sold_date_sk 
      ) 
limit 100;

-- end query 1 in stream 0 using template ../query_templates/query32.tpl
