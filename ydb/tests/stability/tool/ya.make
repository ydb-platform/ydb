PY3_PROGRAM()

PY_SRCS(
    __main__.py
)

DATA(
    arcadia/ydb/tests/stability/resources
)

DEPENDS(
    ydb/apps/ydb
    ydb/tools/cfg/bin
    ydb/tests/tools/nemesis/driver
)

BUNDLE(
    ydb/tests/stress/simple_queue NAME simple_queue
    ydb/tests/stress/olap_workload NAME olap_workload
    ydb/tests/stress/oltp_workload NAME oltp_workload
    ydb/tests/stress/statistics_workload NAME statistics_workload
    ydb/tests/stress/node_broker NAME node_broker_workload
    ydb/tests/stress/transfer NAME transfer_workload
    ydb/tests/stress/s3_backups NAME s3_backups_workload
    ydb/tools/cfg/bin NAME cfg
    ydb/tests/tools/nemesis/driver NAME nemesis
    ydb/apps/ydb NAME ydb_cli
)

RESOURCE(
    ydb_cli ydb_cli
    simple_queue simple_queue
    olap_workload olap_workload
    oltp_workload oltp_workload
    statistics_workload statistics_workload
    node_broker_workload node_broker_workload
    transfer_workload transfer_workload
    s3_backups_workload s3_backups_workload
    cfg cfg
    nemesis nemesis
    ydb/tests/stability/resources/tbl_profile.txt tbl_profile.txt
)


PEERDIR(
    ydb/tests/library
    ydb/tests/library/wardens
)

END()

