FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SET(ORIG_SRC_DIR yt/yql/providers/yt/comp_nodes/ut)

SET(ORIG_SOURCES
    yql_mkql_output_ut.cpp
)

PEERDIR(
    library/cpp/random_provider
    library/cpp/time_provider
    yql/essentials/public/udf/service/exception_policy
    yt/yql/providers/yt/codec
    yt/yql/providers/yt/codec/codegen
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

COPY(
    WITH_CONTEXT
    AUTO
    FROM ${ORIG_SRC_DIR}
    ${ORIG_SOURCES}
)
