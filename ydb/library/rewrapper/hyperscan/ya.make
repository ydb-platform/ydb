LIBRARY()

PEERDIR(
    library/cpp/regex/hyperscan
    ydb/library/rewrapper
)

SRCS(
    GLOBAL hyperscan.cpp
)

END()

