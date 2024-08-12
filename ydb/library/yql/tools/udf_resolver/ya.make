IF (YQL_PACKAGED)
    PACKAGE()

    FROM_SANDBOX(
        FILE {FILE_RESOURCE_ID} OUT_NOAUTO
            udf_resolver
            EXECUTABLE
    )

    END()
ELSE()
    PROGRAM()

    SRCS(
        udf_resolver.cpp
        discover.cpp
    )

    IF (OS_LINUX)
        # prevent external python extensions to lookup protobuf symbols (and maybe
        # other common stuff) in main binary
        EXPORTS_SCRIPT(${ARCADIA_ROOT}/ydb/library/yql/tools/exports.symlist)
        PEERDIR(
            contrib/libs/libc_compat
        )
    ENDIF()

    PEERDIR(
        library/cpp/getopt
        library/cpp/protobuf/util
        ydb/library/yql/minikql
        ydb/library/yql/public/udf/service/terminate_policy
        ydb/library/yql/core
        ydb/library/yql/providers/common/proto
        ydb/library/yql/providers/common/schema/mkql
        ydb/library/yql/utils/backtrace
        ydb/library/yql/utils/sys
        ydb/library/yql/sql/pg_dummy
    )

    YQL_LAST_ABI_VERSION()

    END()
ENDIF()
