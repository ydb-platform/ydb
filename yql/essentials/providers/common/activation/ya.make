LIBRARY()

ENABLE(YQL_STYLE_CPP)

SRCS(
    yql_activation.cpp
)

PEERDIR(
    yql/essentials/providers/common/proto
    library/cpp/svnversion
)

END()
