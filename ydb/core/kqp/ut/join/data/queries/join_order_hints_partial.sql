PRAGMA TablePathPrefix='/Root/test/ds';

-- Partial order hint: item must be in the left subtree of any join where store is on the right.
-- The warehouse->store edge gets widened from (warehouse, store) to (warehouse|item, store),
-- forcing item to be joined with warehouse before store can be introduced.
PRAGMA ydb.OptimizerHints='JoinOrder({item} store)';

select * from item
join warehouse on warehouse.w_warehouse_id = item.i_item_id
join store on store.s_store_id = warehouse.w_warehouse_id
join date_dim on date_dim.d_date_id = store.s_store_id;
