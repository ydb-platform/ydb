{% include 'header.sql.jinja' %}

-- NB: Subquerys
$ssr =
 (select  store.s_store_id as store_id,
          sum(ss_ext_sales_price) as sales,
          sum(coalesce(sr_return_amt, $z0)) as returns,
          sum(ss_net_profit - coalesce(sr_net_loss, $z0)) as profit
  from {{store_sales}} as store_sales
  left join {{store_returns}} as store_returns on
         (store_sales.ss_item_sk = store_returns.sr_item_sk and store_sales.ss_ticket_number = store_returns.sr_ticket_number) cross join
     {{date_dim}} as date_dim cross join
     {{store}} as store cross join
     {{item}} as item cross join
     {{promotion}} as promotion
 where ss_sold_date_sk = d_date_sk
       and cast(d_date as date) between cast('2002-08-06' as date)
                  and (cast('2002-08-06' as date) + DateTime::IntervalFromDays(30))
       and ss_store_sk = s_store_sk
       and ss_item_sk = i_item_sk
       and i_current_price > 50
       and ss_promo_sk = p_promo_sk
       and p_channel_tv = 'N'
 group by store.s_store_id);
 $csr=
 (select  catalog_page.cp_catalog_page_id as catalog_page_id,
          sum(cs_ext_sales_price) as sales,
          sum(coalesce(cr_return_amount, $z0)) as returns,
          sum(cs_net_profit - coalesce(cr_net_loss, $z0)) as profit
  from {{catalog_sales}} as catalog_sales left join {{catalog_returns}} as catalog_returns on
         (catalog_sales.cs_item_sk = catalog_returns.cr_item_sk and catalog_sales.cs_order_number = catalog_returns.cr_order_number) cross join
     {{date_dim}} as date_dim cross join
     {{catalog_page}} as catalog_page cross join
     {{item}} as item cross join
     {{promotion}} as promotion
 where cs_sold_date_sk = d_date_sk
       and cast(d_date as date) between cast('2002-08-06' as date)
                  and (cast('2002-08-06' as date) + DateTime::IntervalFromDays(30))
        and cs_catalog_page_sk = cp_catalog_page_sk
       and cs_item_sk = i_item_sk
       and i_current_price > 50
       and cs_promo_sk = p_promo_sk
       and p_channel_tv = 'N'
group by catalog_page.cp_catalog_page_id)
 ;
 $wsr =
 (select  web_site.web_site_id web_site_id,
          sum(ws_ext_sales_price) as sales,
          sum(coalesce(wr_return_amt, $z0)) as returns,
          sum(ws_net_profit - coalesce(wr_net_loss, $z0)) as profit
  from {{web_sales}} as web_sales
  left outer join {{web_returns}} as web_returns on
         (web_sales.ws_item_sk = web_returns.wr_item_sk and web_sales.ws_order_number = web_returns.wr_order_number) cross join
     {{date_dim}} as date_dim cross join
     {{web_site}} as web_site cross join
     {{item}} as item cross join
     {{promotion}} as promotion
 where ws_sold_date_sk = d_date_sk
       and cast(d_date as date) between cast('2002-08-06' as date)
                  and (cast('2002-08-06' as date) + DateTime::IntervalFromDays(30))
        and ws_web_site_sk = web_site_sk
       and ws_item_sk = i_item_sk
       and i_current_price > 50
       and ws_promo_sk = p_promo_sk
       and p_channel_tv = 'N'
group by web_site.web_site_id);
-- start query 1 in stream 0 using template query80.tpl and seed 1819994127
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
 from   $ssr ssr
 union all
 select 'catalog channel' as channel
        , 'catalog_page' || catalog_page_id as id
        , sales
        , returns
        , profit
 from  $csr csr
 union all
 select 'web channel' as channel
        , 'web_site' || web_site_id as id
        , sales
        , returns
        , profit
 from   $wsr wsr
 ) x
 group by rollup (channel, id)
 order by channel
         ,id
 limit 100;

-- end query 1 in stream 0 using template query80.tpl
