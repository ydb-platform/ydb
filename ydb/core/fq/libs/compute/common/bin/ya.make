PROGRAM(plan)

PEERDIR(
    ydb/core/fq/libs/compute/common
    ydb/core/fq/libs/control_plane_storage/internal
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg_dummy
)

SRCS(
    main.cpp
)

YQL_LAST_ABI_VERSION()

END()

