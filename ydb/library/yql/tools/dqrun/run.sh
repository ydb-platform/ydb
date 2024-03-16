#ya make ~/arcadia/ydb/library/yql/udfs/common/clickhouse/client
#ya make ~/arcadia/ydb/library/yql/udfs/common/re2
./dqrun --gateways-cfg gw.conf --fs-cfg fs.conf -p read_solomon2.sql -s -R
# --udfs-dir ~/arcadia/ydb/library/yql/udfs/common -R
# --expr-file expr.log --trace-opt
# ./dqrun -s --gateways-cfg gw.conf --udfs-dir ~/arcadia/ydb/library/yql/udfs/common -R -p ~/arcadia/kikimr/yq/tpc/h/queries/01_Pricing_Summary_Report_Query.sql