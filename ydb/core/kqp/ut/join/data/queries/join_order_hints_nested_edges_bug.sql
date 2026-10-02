PRAGMA TablePathPrefix='/Root/test/ds';
PRAGMA ydb.OptimizerHints='JoinOrder((item warehouse) store)';

select * from item
inner join warehouse on warehouse.w_warehouse_id = item.i_item_id
inner join store on store.s_store_id = item.i_item_id and store.s_store_id = warehouse.w_warehouse_id;
