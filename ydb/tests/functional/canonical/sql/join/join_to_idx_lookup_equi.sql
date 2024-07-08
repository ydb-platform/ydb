PRAGMA ydb.OverrideStatistics='{"/local/base_join_join_to_idx_lookup_equi_sql_plan/InputJoin1":{"n_rows":7},"/local/base_join_join_to_idx_lookup_equi_sql_plan/InputJoin2":{"n_rows":9},"/local/base_join_join_to_idx_lookup_equi_sql_plan/InputJoin3":{"n_rows":3}}';
PRAGMA DisableSimpleColumns;

SELECT *
FROM InputJoin1 AS t1
JOIN InputJoin2 AS t2 ON t1.Fk21 = t2.Key1 AND t1.Fk22 = t2.Key2
LEFT JOIN InputJoin3 AS t3 ON t2.Key1 = t3.Value
ORDER BY t1.Value;
