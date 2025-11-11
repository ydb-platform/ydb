SET(
    UDFS
    yql/essentials/udfs/common/datetime2
    yql/essentials/udfs/common/digest
    yql/essentials/udfs/common/file
    yql/essentials/udfs/common/hyperloglog
    yql/essentials/udfs/common/pire
    yql/essentials/udfs/common/protobuf
    yql/essentials/udfs/common/python/python3_small
    yql/essentials/udfs/common/re2
    yql/essentials/udfs/common/set
    yql/essentials/udfs/common/stat
    yql/essentials/udfs/common/topfreq
    yql/essentials/udfs/common/top
    yql/essentials/udfs/common/string
    yql/essentials/udfs/common/histogram
    yql/essentials/udfs/common/json2
    yql/essentials/udfs/common/yson2
    yql/essentials/udfs/common/math
    yql/essentials/udfs/common/url_base
    yql/essentials/udfs/common/unicode_base
    yql/essentials/udfs/common/streaming
    yql/essentials/udfs/common/vector
    yql/essentials/udfs/examples/callables
    yql/essentials/udfs/examples/dicts
    yql/essentials/udfs/examples/dummylog
    yql/essentials/udfs/examples/lists
    yql/essentials/udfs/examples/structs
    yql/essentials/udfs/examples/type_inspection
    yql/essentials/udfs/logs/dsv
    yql/essentials/udfs/test/simple
    yql/essentials/udfs/test/test_import
)

IF (OS_LINUX AND CLANG)
    SET(
        UDFS
        ${UDFS}
        yql/essentials/udfs/common/hyperscan
    )
ENDIF()

PACKAGE()

IF (SANITIZER_TYPE != "undefined")

PEERDIR(
    ${UDFS}
)

ENDIF()

END()
