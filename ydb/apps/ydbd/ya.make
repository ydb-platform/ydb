PROGRAM(ydbd)

OWNER(g:kikimr)

IF (OS_LINUX)
    ALLOCATOR(TCMALLOC_256K)
ELSE()
    IF (PROFILE_MEMORY_ALLOCATIONS)
        ALLOCATOR(LF_DBG)
    ELSE()
        ALLOCATOR(LF_YT)
    ENDIF()
ENDIF()

IF (OS_WINDOWS)
    CFLAGS(
        -DKIKIMR_DISABLE_S3_OPS
    )
ENDIF()

SRCS(
    export.cpp
    export.h
    sqs.cpp
    sqs.h
    main.cpp
)

PEERDIR(
    ydb/core/driver_lib/run
    ydb/core/protos
    ydb/core/security
    ydb/core/yq/libs/audit/mock
    ydb/library/folder_service/mock
    ydb/library/keys
    ydb/library/pdisk_io
    ydb/library/security
    ydb/public/sdk/cpp/client/ydb_persqueue_public/codecs
    ydb/library/yql/udfs/common/clickhouse/client
    ydb/library/yql/udfs/common/datetime
    ydb/library/yql/udfs/common/datetime2
    ydb/library/yql/udfs/common/digest
    ydb/library/yql/udfs/common/histogram
    ydb/library/yql/udfs/common/hyperloglog
    ydb/library/yql/udfs/common/hyperscan
    ydb/library/yql/udfs/common/ip_base
    ydb/library/yql/udfs/common/json
    ydb/library/yql/udfs/common/json2
    ydb/library/yql/udfs/common/math
    ydb/library/yql/udfs/common/pire
    ydb/library/yql/udfs/common/re2
    ydb/library/yql/udfs/common/set
    ydb/library/yql/udfs/common/stat
    ydb/library/yql/udfs/common/string
    ydb/library/yql/udfs/common/top
    ydb/library/yql/udfs/common/topfreq
    ydb/library/yql/udfs/common/unicode_base
    ydb/library/yql/udfs/common/url_base
    ydb/library/yql/udfs/common/yson2
    ydb/library/yql/udfs/logs/dsv
)

#
# DON'T ALLOW NEW DEPENDENCIES WITHOUT EXPLICIT APPROVE FROM  kikimr-dev@ or fomichev@
#
CHECK_DEPENDENT_DIRS(
    ALLOW_ONLY
    PEERDIRS
    arc/api/public
    build/external_resources/antlr3
    build/platform
    certs
    contrib
    library
    tools/archiver
    tools/enum_parser/enum_parser
    tools/enum_parser/enum_serialization_runtime
    tools/rescompressor
    tools/rorescompiler
    util
    ydb
    yql
)

YQL_LAST_ABI_VERSION()

IF (OPENSOURCE)
    DISABLE(USE_ASMLIB)
    RESTRICT_LICENSES(
        DENY
        REQUIRE_DISCLOSURE
        FORBIDDEN
        # https://st.yandex-team.ru/DTCC-553
        EXCEPT
        contrib/libs/linux-headers # DTCC-725
        EXCEPT contrib/libs/llvm12/include
    )
ENDIF()

END()
