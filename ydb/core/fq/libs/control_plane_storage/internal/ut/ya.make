UNITTEST_FOR(ydb/core/fq/libs/control_plane_storage/internal)

OWNER(g:yq)

SIZE(MEDIUM)

SRCS(utils_ut.cpp)

PEERDIR(
    library/cpp/testing/unittest
    library/cpp/json/yson
    ydb/library/yql/public/udf/service/stub
)

YQL_LAST_ABI_VERSION()

END()

