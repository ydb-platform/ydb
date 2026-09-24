LIBRARY(run)

ADDINCL(
    ydb/public/sdk/cpp
)

SRCS(
    columnshard_services.cpp
    full_runner.cpp
    main.cpp
)

PEERDIR(
    ydb/core/driver_lib/run/common
    ydb/core/tx/conveyor_composite/service
    ydb/core/tx/priorities/service
    ydb/core/tx/columnshard
    ydb/core/tx/columnshard/data_accessor/cache_policy
    ydb/core/tx/columnshard/column_fetching
    ydb/services/udf_store/compile_controller
)

DEFAULT(YDB_EMBEDDED_NBS_ENABLED yes)

IF (OS_LINUX AND YDB_EMBEDDED_NBS_ENABLED)
    CFLAGS(
        -DYDB_EMBEDDED_NBS_ENABLED
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)

RECURSE_ROOT_RELATIVE(
    ydb/core
    ydb/services
)
