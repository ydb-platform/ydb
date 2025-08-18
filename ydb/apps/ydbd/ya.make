PROGRAM(ydbd)

IF (NOT SANITIZER_TYPE)  # for some reasons some tests with asan are failed, see comment in CPPCOM-32
    NO_EXPORT_DYNAMIC_SYMBOLS()
ENDIF()

IF (OS_LINUX)
    ALLOCATOR(TCMALLOC_256K)
ENDIF()

IF (OS_DARWIN)
    STRIP()
    NO_SPLIT_DWARF()
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
        yql/essentials/udfs/common/hyperscan
    )
ENDIF()

PEERDIR(
    ydb/apps/version
    ydb/core/driver_lib/run
    ydb/core/protos
    ydb/core/security
    ydb/core/tx/schemeshard
    ydb/core/ymq/actor
    ydb/core/ymq/base
    ydb/library/folder_service/mock
    ydb/library/keys
    ydb/library/pdisk_io
    ydb/library/security
    yql/essentials/parser/pg_wrapper
    yql/essentials/sql/pg
    ydb/library/yql/udfs/common/clickhouse/client
    yql/essentials/udfs/common/compress_base
    ydb/library/yql/udfs/common/datetime
    yql/essentials/udfs/common/datetime2
    yql/essentials/udfs/common/digest
    yql/essentials/udfs/common/histogram
    yql/essentials/udfs/common/hyperloglog
    yql/essentials/udfs/common/ip_base
    ydb/library/yql/udfs/common/knn
    ydb/library/yql/udfs/common/roaring
    yql/essentials/udfs/common/json
    yql/essentials/udfs/common/json2
    yql/essentials/udfs/common/math
    yql/essentials/udfs/common/pire
    yql/essentials/udfs/common/re2
    yql/essentials/udfs/common/set
    yql/essentials/udfs/common/stat
    yql/essentials/udfs/common/string
    yql/essentials/udfs/common/top
    yql/essentials/udfs/common/topfreq
    yql/essentials/udfs/common/unicode_base
    yql/essentials/udfs/common/url_base
    yql/essentials/udfs/common/yson2
    yql/essentials/udfs/logs/dsv
    ydb/library/breakpad
)

YQL_LAST_ABI_VERSION()

END()
