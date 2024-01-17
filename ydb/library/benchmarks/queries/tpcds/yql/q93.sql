{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query93.tpl and seed 1200409435
select  ss_customer_sk
            ,sum(act_sales) sumsales
      from (select ss_item_sk
                  ,ss_ticket_number
                  ,ss_customer_sk
                  ,case when sr_return_quantity is not null then (ss_quantity-sr_return_quantity)*ss_sales_price
                                                            else (ss_quantity*ss_sales_price) end act_sales
            from {{store_sales}} as store_sales
            left join {{store_returns}} as store_returns on (store_returns.sr_item_sk = store_sales.ss_item_sk
                                                               and store_returns.sr_ticket_number = store_sales.ss_ticket_number)
                cross join {{reason}} as reason
            where sr_reason_sk = r_reason_sk
              and r_reason_desc = 'reason 66') t
      group by ss_customer_sk
      order by sumsales, ss_customer_sk
limit 100;

-- end query 1 in stream 0 using template query93.tpl
