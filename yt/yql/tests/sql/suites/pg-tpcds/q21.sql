--!syntax_pg
--TPC-DS Q21

-- start query 1 in stream 0 using template ../query_templates/query21.tpl
select  *
 from(select w_warehouse_name
            ,i_item_id
            ,sum(case when (cast(d_date as date) < cast ('1998-04-08' as date))
	                then inv_quantity_on_hand 
                      else 0 end) as inv_before
            ,sum(case when (cast(d_date as date) >= cast ('1998-04-08' as date))
                      then inv_quantity_on_hand 
                      else 0 end) as inv_after
   from plato.inventory
       ,plato.warehouse
       ,plato.item
       ,plato.date_dim
   where i_current_price between 0.99::numeric and 1.49::numeric
     and i_item_sk          = inv_item_sk
     and inv_warehouse_sk   = w_warehouse_sk
     and inv_date_sk    = d_date_sk
     and d_date between (cast ('1998-04-08' as date) - interval '30' day)::date
                    and (cast ('1998-04-08' as date) + interval '30' day)::date
   group by w_warehouse_name, i_item_id) x
 where (case when inv_before > 0 
             then inv_after / inv_before 
             else null::int8
             end) between (2.0/3.0)::int8 and (3.0/2.0)::int8
 order by w_warehouse_name
         ,i_item_id
 limit 100;

-- end query 1 in stream 0 using template ../query_templates/query21.tpl
