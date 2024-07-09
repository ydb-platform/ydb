{% include 'header.sql.jinja' %}

$ssr = (select s.s_store_id as s_store_id,
        SUM(sales_price) as sales,
        SUM(profit) as profit,
        SUM(return_amt) as returns,
        SUM(net_loss) as profit_loss
 from
  ( select  ss_store_sk as store_sk,
            ss_sold_date_sk  as date_sk,
            ss_ext_sales_price as sales_price,
            ss_net_profit as profit,
            $z0 as return_amt,
            $z0 as net_loss
    from {{store_sales}} as ss
    union all
    select sr_store_sk as store_sk,
           sr_returned_date_sk as date_sk,
           $z0 as sales_price,
           $z0 as profit,
           sr_return_amt as return_amt,
           sr_net_loss as net_loss
    from {{store_returns}} as sr
   ) as salesreturns
     cross join {{date_dim}} as d
     cross join {{store}} as s
 where date_sk = d_date_sk
       and cast(d_date as date) between cast('2000-08-19' as date)
                  and (cast('2000-08-19' as date) +  DateTime::IntervalFromDays(14))
       and store_sk = s_store_sk
 group by s.s_store_id);

$csr = (select cp.cp_catalog_page_id as cp_catalog_page_id,
        sum(sales_price) as sales,
        sum(profit) as profit,
        sum(return_amt) as returns,
        sum(net_loss) as profit_loss
 from
  ( select  cs_catalog_page_sk as page_sk,
            cs_sold_date_sk  as date_sk,
            cs_ext_sales_price as sales_price,
            cs_net_profit as profit,
            $z0 as return_amt,
            $z0 as net_loss
    from {{catalog_sales}} as cs
    union all
    select cr_catalog_page_sk as page_sk,
           cr_returned_date_sk as date_sk,
           $z0 as sales_price,
           $z0 as profit,
           cr_return_amount as return_amt,
           cr_net_loss as net_loss
    from {{catalog_returns}} as cr
   ) as salesreturns
     cross join {{date_dim}} as d
     cross join {{catalog_page}} as cp
 where date_sk = d_date_sk
       and cast(d_date as date) between cast('2000-08-19' as date)
                  and (cast('2000-08-19' as date) +  DateTime::IntervalFromDays(14))
       and page_sk = cp_catalog_page_sk
 group by cp.cp_catalog_page_id);

$salesreturns = ( select  ws_web_site_sk as wsr_web_site_sk,
            ws_sold_date_sk  as date_sk,
            ws_ext_sales_price as sales_price,
            ws_net_profit as profit,
            $z0 as return_amt,
            $z0 as net_loss
    from {{web_sales}} as ws
    union all
    select ws_web_site_sk as wsr_web_site_sk,
           wr_returned_date_sk as date_sk,
           $z0 as sales_price,
           $z0 as profit,
           wr_return_amt as return_amt,
           wr_net_loss as net_loss
    from {{web_returns}} as wr
    left outer join {{web_sales}} as ws on
         ( wr.wr_item_sk = ws.ws_item_sk
           and wr.wr_order_number = ws.ws_order_number)
   );

$wsr = (select ws.web_site_id as web_site_id,
        SUM(sales_price) as sales,
        SUM(profit) as profit,
        SUM(return_amt) as returns,
        SUM(net_loss) as profit_loss
 from $salesreturns as s
     cross join {{date_dim}} as d
     cross join {{web_site}} as ws
 where date_sk = d_date_sk
       and cast(d_date as date) between cast('2000-08-19' as date)
                  and (cast('2000-08-19' as date) +  DateTime::IntervalFromDays(14))
       and wsr_web_site_sk = web_site_sk
 group by ws.web_site_id);

$x =  (select 'store channel' as channel
        , 'store' || s_store_id as id
        , sales
        , returns
        , (profit - profit_loss) as profit
 from $ssr as ssr
 union all
 select 'catalog channel' as channel
        , 'catalog_page' || cp_catalog_page_id as id
        , sales
        , returns
        , (profit - profit_loss) as profit
 from  $csr
 union all
 select 'web channel' as channel
        , 'web_site' || web_site_id as id
        , sales
        , returns
        , (profit - profit_loss) as profit
 from  $wsr as wsr
 );

-- start query 1 in stream 0 using template query5.tpl and seed 1819994127





select  channel
        , id
        , SUM(sales) as sales
        , SUM(returns) as returns
        , SUM(profit) as profit
 from $x as x
 group by rollup (channel, id)
 order by channel
         ,id
 limit 100;

-- end query 1 in stream 0 using template query5.tpl
