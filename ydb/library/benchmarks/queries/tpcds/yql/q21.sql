{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query21.tpl and seed 1819994127
select  *
 from(select warehouse.w_warehouse_name w_warehouse_name
            ,item.i_item_id i_item_id
            ,sum(case when (cast(d_date as date) < cast ('1999-03-20' as date))
	                then inv_quantity_on_hand
                      else 0 end) as inv_before
            ,sum(case when (cast(d_date as date) >= cast ('1999-03-20' as date))
                      then inv_quantity_on_hand
                      else 0 end) as inv_after
   from {{inventory}} as inventory
       cross join {{warehouse}} as warehouse
       cross join {{item}} as item
       cross join {{date_dim}} as date_dim
   where i_current_price between $z0_99 and $z1_49
     and i_item_sk          = inv_item_sk
     and inv_warehouse_sk   = w_warehouse_sk
     and inv_date_sk    = d_date_sk
     and cast (d_date as date) between (cast ('1999-03-20' as date) - DateTime::IntervalFromDays(30))
                    and (cast ('1999-03-20' as date) + DateTime::IntervalFromDays(30))
   group by warehouse.w_warehouse_name, item.i_item_id) x
 where (case when inv_before > 0
             then inv_after / inv_before
             else null
             end) between 2.0/3.0 and 3.0/2.0
 order by w_warehouse_name
         ,i_item_id
 limit 100;

-- end query 1 in stream 0 using template query21.tpl
