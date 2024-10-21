PRAGMA ydb.OptOverrideStatistics='{"/local/base_join_join_to_idx_lookup_partial_left_sql_plan/InputJoin1":{"n_rows":7},"/local/base_join_join_to_idx_lookup_partial_left_sql_plan/InputJoin2":{"n_rows":9},"/local/base_join_join_to_idx_lookup_partial_left_sql_plan/InputJoin3":{"n_rows":3}}';

SELECT t1.Value AS Value1, t2.Value AS Value2, t3.Value AS Value3
FROM InputJoin1 AS t1
LEFT JOIN InputJoin2 AS t2
ON t1.Fk21 == t2.Key1
LEFT JOIN InputJoin3 AS t3
ON t2.Fk3 == t3.Key
WHERE t1.Value != "Value2"
ORDER BY Value1, Value2;
