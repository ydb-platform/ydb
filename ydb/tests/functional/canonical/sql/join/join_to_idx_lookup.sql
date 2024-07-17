PRAGMA ydb.OverrideStatistics='{"/local/base_join_join_to_idx_lookup_sql_plan/InputJoin1":{"n_rows":7},"/local/base_join_join_to_idx_lookup_sql_plan/InputJoin2":{"n_rows":9},"/local/base_join_join_to_idx_lookup_sql_plan/InputJoin3":{"n_rows":3}}';

SELECT t3.Value FROM InputJoin1 AS t1
LEFT JOIN InputJoin2 AS t2
ON t1.Fk21 == t2.Key1 AND t1.Fk22 == t2.Key2
LEFT JOIN InputJoin3 AS t3
ON t2.Fk3 == t3.Key
WHERE t1.Value == "Value2" OR t1.Value == "Value3";
