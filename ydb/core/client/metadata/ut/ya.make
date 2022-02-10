UNITTEST_FOR(ydb/core/client/metadata)

OWNER(g:kikimr)

SRCS(
    functions_metadata_ut.cpp
)

PEERDIR(
    ydb/library/yql/public/udf/service/stub
)

YQL_LAST_ABI_VERSION()
 
END()
