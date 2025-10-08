LIBRARY()

ENABLE(YQL_STYLE_CPP)

SRCS(
    yql_qstorage.cpp
)

PEERDIR(
    library/cpp/threading/future
)

END()
