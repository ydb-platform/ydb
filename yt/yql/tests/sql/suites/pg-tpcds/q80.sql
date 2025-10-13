--!syntax_pg
--TPC-DS Q80

-- start query 1 in stream 0 using template ../query_templates/query80.tpl
with ssr as
 (select  s_store_id as store_id,
          sum(ss_ext_sales_price) as sales,
          sum(coalesce(sr_return_amt, 0::numeric)) as returns,
          sum(ss_net_profit - coalesce(sr_net_loss, 0::numeric)) as profit
  from plato.store_sales left outer join plato.store_returns on
         (ss_item_sk = sr_item_sk and ss_ticket_number = sr_ticket_number),
     plato.date_dim,
     plato.store,
     plato.item,
     plato.promotion
 where ss_sold_date_sk = d_date_sk
       and d_date between cast('1998-08-04' as date) 
                  and (cast('1998-08-04' as date) + interval '30' day)::date
       and ss_store_sk = s_store_sk
       and ss_item_sk = i_item_sk
       and i_current_price > 50::numeric
       and ss_promo_sk = p_promo_sk
       and p_channel_tv = 'N'
 group by s_store_id)
 ,
 csr as
 (select  cp_catalog_page_id as catalog_page_id,
          sum(cs_ext_sales_price) as sales,
          sum(coalesce(cr_return_amount, 0::numeric)) as returns,
          sum(cs_net_profit - coalesce(cr_net_loss, 0::numeric)) as profit
  from plato.catalog_sales left outer join plato.catalog_returns on
         (cs_item_sk = cr_item_sk and cs_order_number = cr_order_number),
     plato.date_dim,
     plato.catalog_page,
     plato.item,
     plato.promotion
 where cs_sold_date_sk = d_date_sk
       and d_date between cast('1998-08-04' as date)
                  and (cast('1998-08-04' as date) + interval '30' day)::date
        and cs_catalog_page_sk = cp_catalog_page_sk
       and cs_item_sk = i_item_sk
       and i_current_price > 50::numeric
       and cs_promo_sk = p_promo_sk
       and p_channel_tv = 'N'
group by cp_catalog_page_id)
 ,
 wsr as
 (select  web_site_id,
          sum(ws_ext_sales_price) as sales,
          sum(coalesce(wr_return_amt, 0::numeric)) as returns,
          sum(ws_net_profit - coalesce(wr_net_loss, 0::numeric)) as profit
  from plato.web_sales left outer join plato.web_returns on
         (ws_item_sk = wr_item_sk and ws_order_number = wr_order_number),
     plato.date_dim,
     plato.web_site,
     plato.item,
     plato.promotion
 where ws_sold_date_sk = d_date_sk
       and d_date between cast('1998-08-04' as date)
                  and (cast('1998-08-04' as date) + interval '30' day)::date
        and ws_web_site_sk = web_site_sk
       and ws_item_sk = i_item_sk
       and i_current_price > 50::numeric
       and ws_promo_sk = p_promo_sk
       and p_channel_tv = 'N'
group by web_site_id)
  select  channel
        , id
        , sum(sales) as sales
        , sum(returns) as returns
        , sum(profit) as profit
 from 
 (select 'store channel' as channel
        , 'store' || store_id as id
        , sales
        , returns
        , profit
 from   ssr
 union all
 select 'catalog channel' as channel
        , 'catalog_page' || catalog_page_id as id
        , sales
        , returns
        , profit
 from  csr
 union all
 select 'web channel' as channel
        , 'web_site' || web_site_id as id
        , sales
        , returns
        , profit
 from   wsr
 ) x
 group by rollup (channel, id)
 order by channel
         ,id
 limit 100;

-- end query 1 in stream 0 using template ../query_templates/query80.tpl
