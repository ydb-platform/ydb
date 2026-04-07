LIBRARY()

SRCS(
    solomon_emulator_helpers.cpp
)

PEERDIR(
    library/cpp/http/simple
    library/cpp/retry
)

YQL_LAST_ABI_VERSION()

END()
