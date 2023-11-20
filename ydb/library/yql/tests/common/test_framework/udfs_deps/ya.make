SET(
    UDFS
    ydb/library/yql/udfs/common/datetime2
    ydb/library/yql/udfs/common/digest
    ydb/library/yql/udfs/common/file
    ydb/library/yql/udfs/common/hyperloglog
    ydb/library/yql/udfs/common/pire
    ydb/library/yql/udfs/common/protobuf
    ydb/library/yql/udfs/common/re2
    ydb/library/yql/udfs/common/set
    ydb/library/yql/udfs/common/stat
    ydb/library/yql/udfs/common/topfreq
    ydb/library/yql/udfs/common/top
    ydb/library/yql/udfs/common/string
    ydb/library/yql/udfs/common/histogram
    ydb/library/yql/udfs/common/json2
    ydb/library/yql/udfs/common/yson2
    ydb/library/yql/udfs/common/math
    ydb/library/yql/udfs/common/url_base
    ydb/library/yql/udfs/common/unicode_base
    ydb/library/yql/udfs/common/streaming
    ydb/library/yql/udfs/examples/callables
    ydb/library/yql/udfs/examples/dicts
    ydb/library/yql/udfs/examples/dummylog
    ydb/library/yql/udfs/examples/lists
    ydb/library/yql/udfs/examples/structs
    ydb/library/yql/udfs/examples/type_inspection
    ydb/library/yql/udfs/logs/dsv
    ydb/library/yql/udfs/test/simple
    ydb/library/yql/udfs/test/test_import
)

IF (OS_LINUX AND CLANG)
    SET(
        UDFS
        ${UDFS}
        ydb/library/yql/udfs/common/hyperscan
    )
ENDIF()

PACKAGE()

IF (SANITIZER_TYPE != "undefined")

PEERDIR(
    ${UDFS}
)

ENDIF()

END()
