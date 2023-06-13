LIBRARY()

PEERDIR(
    library/cpp/json
    library/cpp/json/yson
    library/cpp/yson
    ydb/library/yql/sql/settings
    ydb/library/yql/utils    
)

SRCS(utils.cpp)

END()

RECURSE_FOR_TESTS(
    ut
)
