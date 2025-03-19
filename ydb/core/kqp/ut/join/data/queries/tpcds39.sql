pragma TablePathPrefix = "/Root/test/ds/";

$inv =
(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
       ,mean, case mean when 0 then null else mean end cov
 from(select warehouse.w_warehouse_name w_warehouse_name,warehouse.w_warehouse_sk w_warehouse_sk,item.i_item_sk i_item_sk,date_dim.d_moy d_moy
            ,avg(inv_quantity_on_hand) mean
      from `/Root/test/ds/inventory` as inverntory
          cross join `/Root/test/ds/item` as item
          cross join `/Root/test/ds/warehouse` as warehouse
          cross join `/Root/test/ds/date_dim` as date_dim
      where inv_item_sk = i_item_sk
        and inv_warehouse_sk = w_warehouse_sk
        and inv_date_sk = d_date_sk
        and d_year =2000
      group by warehouse.w_warehouse_name,warehouse.w_warehouse_sk,item.i_item_sk,date_dim.d_moy) foo
 where case mean when 0 then 0 else mean end > 1);
select inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean, inv1.cov
        ,inv2.w_warehouse_sk,inv2.i_item_sk,inv2.d_moy,inv2.mean, inv2.cov
from $inv inv1 cross join $inv inv2
where inv1.i_item_sk = inv2.i_item_sk
  and inv1.w_warehouse_sk =  inv2.w_warehouse_sk
  and inv1.d_moy=2
  and inv2.d_moy=2+1
order by inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean,inv1.cov
        ,inv2.d_moy,inv2.mean, inv2.cov
;
select inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean, inv1.cov
        ,inv2.w_warehouse_sk,inv2.i_item_sk,inv2.d_moy,inv2.mean, inv2.cov
from $inv inv1 cross join $inv inv2
where inv1.i_item_sk = inv2.i_item_sk
  and inv1.w_warehouse_sk =  inv2.w_warehouse_sk
  and inv1.d_moy=2
  and inv2.d_moy=2+1
  and inv1.cov > 1.5
order by inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean,inv1.cov
        ,inv2.d_moy,inv2.mean, inv2.cov
;