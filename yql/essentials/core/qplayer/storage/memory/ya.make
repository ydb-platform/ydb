LIBRARY()

ENABLE(YQL_STYLE_CPP)

SRCS(
    yql_qstorage_memory.cpp
)

PEERDIR(
    yql/essentials/core/qplayer/storage/interface
)

END()

RECURSE_FOR_TESTS(
    ut
)
