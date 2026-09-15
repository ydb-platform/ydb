PRAGMA TablePathPrefix='/Root/test/ds';

-- Two hints whose node sets partially overlap with the complex edge {item,warehouse}->store.
-- Hint 1 (item,ship_mode): Overlaps with Left={item,warehouse} on 'item' but not a subset.
-- Hint 2 (warehouse,date_dim): Overlaps with Left={item,warehouse} on 'warehouse' but not a subset.
-- The outer widening loop must use IsSubset (not Overlaps) to avoid spuriously widening
-- the complex edge: old Overlaps code would accumulate {item,warehouse,ship_mode,date_dim}->store,
-- and the second hint would then add 'warehouse' to the Right side (creating an edge with
-- warehouse on both Left and Right — a corrupted edge).
PRAGMA ydb.OptimizerHints='JoinOrder(item ship_mode) JoinOrder(warehouse date_dim)';

select * from item
left join warehouse on warehouse.w_warehouse_id = item.i_item_id
left join store on store.s_store_id = item.i_item_id and store.s_store_id = warehouse.w_warehouse_id
left join date_dim on date_dim.d_date_id = warehouse.w_warehouse_id
left join ship_mode on ship_mode.sm_ship_mode_id = item.i_item_id;
