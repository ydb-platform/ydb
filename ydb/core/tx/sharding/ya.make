LIBRARY()

PEERDIR(
    ydb/library/yql/minikql
    ydb/library/yql/utils
    ydb/library/yql/public/udf
    ydb/core/formats/arrow/hash
    ydb/core/tx/schemeshard/olap/schema
    ydb/core/tx/columnshard/common
    ydb/core/formats
    ydb/core/protos
)

YQL_LAST_ABI_VERSION()

SRCS(
    sharding.cpp
    hash.cpp
    unboxed_reader.cpp
    hash_slider.cpp
    GLOBAL hash_modulo.cpp
    GLOBAL hash_intervals.cpp
    random.cpp
)

IF (OS_WINDOWS)
    CFLAGS(
        -DKIKIMR_DISABLE_WINDOWS
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
)