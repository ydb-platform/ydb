PROGRAM(ydbd)

INCLUDE(${ARCADIA_ROOT}/ydb/opensource.inc)

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
    main.cpp
)

IF (ARCH_X86_64)
    PEERDIR(
        ydb/library/yql/udfs/common/hyperscan
    )
ENDIF()

PEERDIR(
    ydb/core/driver_lib/run
    ydb/core/protos
    ydb/core/security
    ydb/core/ymq/actor
    ydb/core/ymq/base
    ydb/library/folder_service/mock
    ydb/library/keys
    ydb/library/pdisk_io
    ydb/library/security
    ydb/library/yql/parser/pg_wrapper
    ydb/library/yql/sql/pg
    ydb/library/yql/udfs/common/clickhouse/client
    ydb/library/yql/udfs/common/datetime
    ydb/library/yql/udfs/common/datetime2
    ydb/library/yql/udfs/common/digest
    ydb/library/yql/udfs/common/histogram
    ydb/library/yql/udfs/common/hyperloglog
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
    ydb/public/sdk/cpp/client/ydb_persqueue_public/codecs
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
)

YQL_LAST_ABI_VERSION()

END()

