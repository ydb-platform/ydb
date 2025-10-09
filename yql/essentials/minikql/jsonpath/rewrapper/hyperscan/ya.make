LIBRARY()

ENABLE(YQL_STYLE_CPP)

PEERDIR(
    library/cpp/regex/hyperscan
    yql/essentials/minikql/jsonpath/rewrapper
)

SRCS(
    GLOBAL hyperscan.cpp
)

END()

