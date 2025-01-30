PRAGMA TablePathPrefix='/Root/test/ds';

--- NB: Subquerys

$cs_ui =

 (select catalog_sales.cs_item_sk

  from catalog_sales as catalog_sales

  group by catalog_sales.cs_item_sk
);

$cross_sales =

 (select item.i_product_name product_name

     ,store.s_store_name store_name

  FROM  $cs_ui cs_ui

        cross join store as store

        cross join item as item

group by item.i_product_name

       ,store.s_store_name

);

-- start query 1 in stream 0 using template query64.tpl and seed 1220860970

select cs1.product_name

from $cross_sales cs1 cross join $cross_sales cs2

where cs1.store_name = cs2.store_name;



-- end query 1 in stream 0 using template query64.tpl