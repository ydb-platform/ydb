{% include 'header.sql.jinja' %}

-- NB: Subquerys

-- start query 1 in stream 0 using template query15.tpl and seed 1819994127
$todecimal = ($x) -> {
  return cast(cast($x as string?) as decimal(7,2))
};

select  customer_address.ca_zip
       ,sum($todecimal(cs_sales_price))
 from {{catalog_sales}} as catalog_sales
     cross join {{customer}} as customer
     cross join {{customer_address}} as customer_address
     cross join {{date_dim}} as date_dim
 where cs_bill_customer_sk = c_customer_sk
 	and c_current_addr_sk = ca_address_sk
 	and ( substring(cast(ca_zip as string),0,5) in ('85669', '86197','88274','83405','86475',
                                   '85392', '85460', '80348', '81792')
 	      or ca_state in ('CA','WA','GA')
 	      or cs_sales_price > 500)
 	and cs_sold_date_sk = d_date_sk
 	and d_qoy = 2 and d_year = 1998
 group by customer_address.ca_zip
 order by customer_address.ca_zip
 limit 100;

-- end query 1 in stream 0 using template query15.tpl
