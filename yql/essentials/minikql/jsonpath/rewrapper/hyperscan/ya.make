LIBRARY()

PEERDIR(
    library/cpp/regex/hyperscan
    yql/essentials/minikql/jsonpath/rewrapper
)

SRCS(
    GLOBAL hyperscan.cpp
)

END()

