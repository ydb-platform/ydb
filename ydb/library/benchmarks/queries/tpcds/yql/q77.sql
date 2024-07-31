{% include 'header.sql.jinja' %}

-- NB: Subquerys
$ss =
 (select store.s_store_sk s_store_sk,
         sum(ss_ext_sales_price) as sales,
         sum(ss_net_profit) as profit
 from {{store_sales}} as store_sales cross join
      {{date_dim}} as date_dim cross join
      {{store}} as store
 where ss_sold_date_sk = d_date_sk
       and cast(d_date as date) between cast('2000-08-16' as date)
                  and (cast('2000-08-16' as date) +  DateTime::IntervalFromDays(30))
       and ss_store_sk = s_store_sk
 group by store.s_store_sk);

 $sr =
 (select store.s_store_sk s_store_sk,
         sum(sr_return_amt) as returns,
         sum(sr_net_loss) as profit_loss
 from {{store_returns}} as store_returns cross join
      {{date_dim}} as date_dim cross join
      {{store}} as store
 where sr_returned_date_sk = d_date_sk
       and cast(d_date as date) between cast('2000-08-16' as date)
                  and (cast('2000-08-16' as date) +  DateTime::IntervalFromDays(30))
       and sr_store_sk = s_store_sk
 group by store.s_store_sk);
 $cs =
 (select catalog_sales.cs_call_center_sk cs_call_center_sk,
        sum(cs_ext_sales_price) as sales,
        sum(cs_net_profit) as profit
 from {{catalog_sales}} as catalog_sales cross join
      {{date_dim}} as date_dim
 where cs_sold_date_sk = d_date_sk
       and cast(d_date as date) between cast('2000-08-16' as date)
                  and (cast('2000-08-16' as date) +  DateTime::IntervalFromDays(30))
 group by catalog_sales.cs_call_center_sk
 );
 $cr =
 (select catalog_returns.cr_call_center_sk cr_call_center_sk,
         sum(cr_return_amount) as returns,
         sum(cr_net_loss) as profit_loss
 from {{catalog_returns}} as catalog_returns cross join
      {{date_dim}} as date_dim
 where cr_returned_date_sk = d_date_sk
       and cast(d_date as date) between cast('2000-08-16' as date)
                  and (cast('2000-08-16' as date) +  DateTime::IntervalFromDays(30))
 group by catalog_returns.cr_call_center_sk
 );
 $ws =
 ( select web_page.wp_web_page_sk wp_web_page_sk,
        sum(ws_ext_sales_price) as sales,
        sum(ws_net_profit) as profit
 from {{web_sales}} as web_sales cross join
      {{date_dim}} as date_dim cross join
      {{web_page}} as web_page
 where ws_sold_date_sk = d_date_sk
       and cast(d_date as date) between cast('2000-08-16' as date)
                  and (cast('2000-08-16' as date) +  DateTime::IntervalFromDays(30))
       and ws_web_page_sk = wp_web_page_sk
 group by web_page.wp_web_page_sk);
 $wr =
 (select web_page.wp_web_page_sk wp_web_page_sk,
        sum(wr_return_amt) as returns,
        sum(wr_net_loss) as profit_loss
 from {{web_returns}} as web_returns cross join
      {{date_dim}} as date_dim cross join
      {{web_page}} as web_page
 where wr_returned_date_sk = d_date_sk
       and cast(d_date as date) between cast('2000-08-16' as date)
                  and (cast('2000-08-16' as date) +  DateTime::IntervalFromDays(30))
       and wr_web_page_sk = wp_web_page_sk
 group by web_page.wp_web_page_sk);
-- start query 1 in stream 0 using template query77.tpl and seed 1819994127
  select  channel
        , id
        , sum(sales) as sales
        , sum(returns) as returns
        , sum(profit) as profit
 from
 (select 'store channel' as channel
        , ss.s_store_sk as id
        , sales
        , coalesce(returns, $z0_35) as returns
        , (profit - coalesce(profit_loss,$z0_35)) as profit
 from   $ss ss left join $sr sr
        on  ss.s_store_sk = sr.s_store_sk
 union all
 select 'catalog channel' as channel
        , cs_call_center_sk as id
        , sales
        , returns
        , (profit - profit_loss) as profit
 from  $cs cs
       cross join $cr cr
 union all
 select 'web channel' as channel
        , ws.wp_web_page_sk as id
        , sales
        , coalesce(returns, $z0_35) returns
        , (profit - coalesce(profit_loss,$z0_35)) as profit
 from   $ws ws left join $wr wr
        on  ws.wp_web_page_sk = wr.wp_web_page_sk
 ) x
 group by rollup (channel, id)
 order by channel
         ,id
         ,sales
 limit 100;

-- end query 1 in stream 0 using template query77.tpl
