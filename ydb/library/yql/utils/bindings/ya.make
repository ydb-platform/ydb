LIBRARY()

PEERDIR(
    library/cpp/json
    library/cpp/json/yson
    library/cpp/yson
    yql/essentials/sql/settings
    yql/essentials/utils    
)

SRCS(utils.cpp)

END()

RECURSE_FOR_TESTS(
    ut
)
