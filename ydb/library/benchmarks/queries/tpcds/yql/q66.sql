{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query66.tpl and seed 2042478054
select
         w_warehouse_name
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
        ,ship_carriers
        ,year
 	,sum(jan_sales) as jan_sales
 	,sum(feb_sales) as feb_sales
 	,sum(mar_sales) as mar_sales
 	,sum(apr_sales) as apr_sales
 	,sum(may_sales) as may_sales
 	,sum(jun_sales) as jun_sales
 	,sum(jul_sales) as jul_sales
 	,sum(aug_sales) as aug_sales
 	,sum(sep_sales) as sep_sales
 	,sum(oct_sales) as oct_sales
 	,sum(nov_sales) as nov_sales
 	,sum(dec_sales) as dec_sales
 	,sum(jan_sales/w_warehouse_sq_ft) as jan_sales_per_sq_foot
 	,sum(feb_sales/w_warehouse_sq_ft) as feb_sales_per_sq_foot
 	,sum(mar_sales/w_warehouse_sq_ft) as mar_sales_per_sq_foot
 	,sum(apr_sales/w_warehouse_sq_ft) as apr_sales_per_sq_foot
 	,sum(may_sales/w_warehouse_sq_ft) as may_sales_per_sq_foot
 	,sum(jun_sales/w_warehouse_sq_ft) as jun_sales_per_sq_foot
 	,sum(jul_sales/w_warehouse_sq_ft) as jul_sales_per_sq_foot
 	,sum(aug_sales/w_warehouse_sq_ft) as aug_sales_per_sq_foot
 	,sum(sep_sales/w_warehouse_sq_ft) as sep_sales_per_sq_foot
 	,sum(oct_sales/w_warehouse_sq_ft) as oct_sales_per_sq_foot
 	,sum(nov_sales/w_warehouse_sq_ft) as nov_sales_per_sq_foot
 	,sum(dec_sales/w_warehouse_sq_ft) as dec_sales_per_sq_foot
 	,sum(jan_net) as jan_net
 	,sum(feb_net) as feb_net
 	,sum(mar_net) as mar_net
 	,sum(apr_net) as apr_net
 	,sum(may_net) as may_net
 	,sum(jun_net) as jun_net
 	,sum(jul_net) as jul_net
 	,sum(aug_net) as aug_net
 	,sum(sep_net) as sep_net
 	,sum(oct_net) as oct_net
 	,sum(nov_net) as nov_net
 	,sum(dec_net) as dec_net
 from (
     select
 	warehouse.w_warehouse_name w_warehouse_name
 	,warehouse.w_warehouse_sq_ft w_warehouse_sq_ft
 	,warehouse.w_city w_city
 	,warehouse.w_county w_county
 	,warehouse.w_state w_state
 	,warehouse.w_country w_country
 	,'DHL' || ',' || 'BARIAN' as ship_carriers
       ,date_dim.d_year as year
 	,sum(case when d_moy = 1
 		then $upscale(ws_ext_sales_price)* $upscale(ws_quantity) else $upscale($z0) end) as jan_sales
 	,sum(case when d_moy = 2
 		then $upscale(ws_ext_sales_price)* $upscale(ws_quantity) else $upscale($z0) end) as feb_sales
 	,sum(case when d_moy = 3
 		then $upscale(ws_ext_sales_price)* $upscale(ws_quantity) else $upscale($z0) end) as mar_sales
 	,sum(case when d_moy = 4
 		then $upscale(ws_ext_sales_price)* $upscale(ws_quantity) else $upscale($z0) end) as apr_sales
 	,sum(case when d_moy = 5
 		then $upscale(ws_ext_sales_price)* $upscale(ws_quantity) else $upscale($z0) end) as may_sales
 	,sum(case when d_moy = 6
 		then $upscale(ws_ext_sales_price)* $upscale(ws_quantity) else $upscale($z0) end) as jun_sales
 	,sum(case when d_moy = 7
 		then $upscale(ws_ext_sales_price)* $upscale(ws_quantity) else $upscale($z0) end) as jul_sales
 	,sum(case when d_moy = 8
 		then $upscale(ws_ext_sales_price)* $upscale(ws_quantity) else $upscale($z0) end) as aug_sales
 	,sum(case when d_moy = 9
 		then $upscale(ws_ext_sales_price)* $upscale(ws_quantity) else $upscale($z0) end) as sep_sales
 	,sum(case when d_moy = 10
 		then $upscale(ws_ext_sales_price)* $upscale(ws_quantity) else $upscale($z0) end) as oct_sales
 	,sum(case when d_moy = 11
 		then $upscale(ws_ext_sales_price)* $upscale(ws_quantity) else $upscale($z0) end) as nov_sales
 	,sum(case when d_moy = 12
 		then $upscale(ws_ext_sales_price)* $upscale(ws_quantity) else $upscale($z0) end) as dec_sales
 	,sum(case when d_moy = 1
 		then $upscale(ws_net_paid)* $upscale(ws_quantity) else $upscale($z0) end) as jan_net
 	,sum(case when d_moy = 2
 		then $upscale(ws_net_paid)* $upscale(ws_quantity) else $upscale($z0) end) as feb_net
 	,sum(case when d_moy = 3
 		then $upscale(ws_net_paid)* $upscale(ws_quantity) else $upscale($z0) end) as mar_net
 	,sum(case when d_moy = 4
 		then $upscale(ws_net_paid)* $upscale(ws_quantity) else $upscale($z0) end) as apr_net
 	,sum(case when d_moy = 5
 		then $upscale(ws_net_paid)* $upscale(ws_quantity) else $upscale($z0) end) as may_net
 	,sum(case when d_moy = 6
 		then $upscale(ws_net_paid)* $upscale(ws_quantity) else $upscale($z0) end) as jun_net
 	,sum(case when d_moy = 7
 		then $upscale(ws_net_paid)* $upscale(ws_quantity) else $upscale($z0) end) as jul_net
 	,sum(case when d_moy = 8
 		then $upscale(ws_net_paid)* $upscale(ws_quantity) else $upscale($z0) end) as aug_net
 	,sum(case when d_moy = 9
 		then $upscale(ws_net_paid)* $upscale(ws_quantity) else $upscale($z0) end) as sep_net
 	,sum(case when d_moy = 10
 		then $upscale(ws_net_paid)* $upscale(ws_quantity) else $upscale($z0) end) as oct_net
 	,sum(case when d_moy = 11
 		then $upscale(ws_net_paid)* $upscale(ws_quantity) else $upscale($z0) end) as nov_net
 	,sum(case when d_moy = 12
 		then $upscale(ws_net_paid)* $upscale(ws_quantity) else $upscale($z0) end) as dec_net
     from
          {{web_sales}} as web_sales
         cross join {{warehouse}} as warehouse
         cross join {{date_dim}} as date_dim
         cross join {{time_dim}} as time_dim
 	  cross join {{ship_mode}} as ship_mode
     where
            ws_warehouse_sk =  w_warehouse_sk
        and ws_sold_date_sk = d_date_sk
        and ws_sold_time_sk = t_time_sk
 	and ws_ship_mode_sk = sm_ship_mode_sk
        and d_year = 2001
 	and t_time between 30838 and 30838+28800
 	and sm_carrier in ('DHL','BARIAN')
     group by
        warehouse.w_warehouse_name
 	,warehouse.w_warehouse_sq_ft
 	,warehouse.w_city
 	,warehouse.w_county
 	,warehouse.w_state
 	,warehouse.w_country
       ,date_dim.d_year
 union all
     select
 	warehouse.w_warehouse_name w_warehouse_name
 	,warehouse.w_warehouse_sq_ft w_warehouse_sq_ft
 	,warehouse.w_city w_city
 	,warehouse.w_county w_county
 	,warehouse.w_state w_state
 	,warehouse.w_country w_country
 	,'DHL' || ',' || 'BARIAN' as ship_carriers
       ,date_dim.d_year as year
 	,sum(case when d_moy = 1
 		then $upscale(cs_sales_price)* $upscale(cs_quantity) else $upscale($z0) end) as jan_sales
 	,sum(case when d_moy = 2
 		then $upscale(cs_sales_price)* $upscale(cs_quantity) else $upscale($z0) end) as feb_sales
 	,sum(case when d_moy = 3
 		then $upscale(cs_sales_price)* $upscale(cs_quantity) else $upscale($z0) end) as mar_sales
 	,sum(case when d_moy = 4
 		then $upscale(cs_sales_price)* $upscale(cs_quantity) else $upscale($z0) end) as apr_sales
 	,sum(case when d_moy = 5
 		then $upscale(cs_sales_price)* $upscale(cs_quantity) else $upscale($z0) end) as may_sales
 	,sum(case when d_moy = 6
 		then $upscale(cs_sales_price)* $upscale(cs_quantity) else $upscale($z0) end) as jun_sales
 	,sum(case when d_moy = 7
 		then $upscale(cs_sales_price)* $upscale(cs_quantity) else $upscale($z0) end) as jul_sales
 	,sum(case when d_moy = 8
 		then $upscale(cs_sales_price)* $upscale(cs_quantity) else $upscale($z0) end) as aug_sales
 	,sum(case when d_moy = 9
 		then $upscale(cs_sales_price)* $upscale(cs_quantity) else $upscale($z0) end) as sep_sales
 	,sum(case when d_moy = 10
 		then $upscale(cs_sales_price)* $upscale(cs_quantity) else $upscale($z0) end) as oct_sales
 	,sum(case when d_moy = 11
 		then $upscale(cs_sales_price)* $upscale(cs_quantity) else $upscale($z0) end) as nov_sales
 	,sum(case when d_moy = 12
 		then $upscale(cs_sales_price)* $upscale(cs_quantity) else $upscale($z0) end) as dec_sales
 	,sum(case when d_moy = 1
 		then $upscale(cs_net_paid_inc_tax)* $upscale(cs_quantity) else $upscale($z0) end) as jan_net
 	,sum(case when d_moy = 2
 		then $upscale(cs_net_paid_inc_tax)* $upscale(cs_quantity) else $upscale($z0) end) as feb_net
 	,sum(case when d_moy = 3
 		then $upscale(cs_net_paid_inc_tax)* $upscale(cs_quantity) else $upscale($z0) end) as mar_net
 	,sum(case when d_moy = 4
 		then $upscale(cs_net_paid_inc_tax)* $upscale(cs_quantity) else $upscale($z0) end) as apr_net
 	,sum(case when d_moy = 5
 		then $upscale(cs_net_paid_inc_tax)* $upscale(cs_quantity) else $upscale($z0) end) as may_net
 	,sum(case when d_moy = 6
 		then $upscale(cs_net_paid_inc_tax)* $upscale(cs_quantity) else $upscale($z0) end) as jun_net
 	,sum(case when d_moy = 7
 		then $upscale(cs_net_paid_inc_tax)* $upscale(cs_quantity) else $upscale($z0) end) as jul_net
 	,sum(case when d_moy = 8
 		then $upscale(cs_net_paid_inc_tax)* $upscale(cs_quantity) else $upscale($z0) end) as aug_net
 	,sum(case when d_moy = 9
 		then $upscale(cs_net_paid_inc_tax)* $upscale(cs_quantity) else $upscale($z0) end) as sep_net
 	,sum(case when d_moy = 10
 		then $upscale(cs_net_paid_inc_tax)* $upscale(cs_quantity) else $upscale($z0) end) as oct_net
 	,sum(case when d_moy = 11
 		then $upscale(cs_net_paid_inc_tax)* $upscale(cs_quantity) else $upscale($z0) end) as nov_net
 	,sum(case when d_moy = 12
 		then $upscale(cs_net_paid_inc_tax)* $upscale(cs_quantity) else $upscale($z0) end) as dec_net
     from
          {{catalog_sales}} as catalog_sales
         cross join {{warehouse}} as warehouse
         cross join {{date_dim}} as date_dim
         cross join {{time_dim}} as time_dim
 	 cross join {{ship_mode}} as ship_mode
     where
            cs_warehouse_sk =  w_warehouse_sk
        and cs_sold_date_sk = d_date_sk
        and cs_sold_time_sk = t_time_sk
 	and cs_ship_mode_sk = sm_ship_mode_sk
        and d_year = 2001
 	and t_time between 30838 AND 30838+28800
 	and sm_carrier in ('DHL','BARIAN')
     group by
        warehouse.w_warehouse_name
 	,warehouse.w_warehouse_sq_ft
 	,warehouse.w_city
 	,warehouse.w_county
 	,warehouse.w_state
 	,warehouse.w_country
       ,date_dim.d_year
 ) x
 group by
        w_warehouse_name
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
 	,ship_carriers
       ,year
 order by w_warehouse_name
 limit 100;

-- end query 1 in stream 0 using template query66.tpl
