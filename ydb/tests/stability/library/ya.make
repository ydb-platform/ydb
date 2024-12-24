PY3_PROGRAM()

PY_SRCS(
    __main__.py
)

DATA(
    arcadia/ydb/tests/stability/resources
)

DEPENDS(
    ydb/tools/cfg/bin
    ydb/tests/tools/nemesis/driver
)

BUNDLE(
    ydb/tests/workloads/simple_queue NAME simple_queue
    ydb/tests/workloads/olap_workload NAME olap_workload
    ydb/tests/workloads/statistics_workload NAME statistics_workload
    ydb/tools/cfg/bin NAME cfg
    ydb/tests/tools/nemesis/driver NAME nemesis
)

RESOURCE(
    simple_queue simple_queue
    olap_workload olap_workload
    statistics_workload statistics_workload
    cfg cfg
    nemesis nemesis
    ydb/tests/stability/resources/tbl_profile.txt tbl_profile.txt
)


PEERDIR(
    ydb/tests/library
    ydb/tests/library/wardens
)

END()

