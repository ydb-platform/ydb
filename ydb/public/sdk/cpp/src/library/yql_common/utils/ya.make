LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    utf8.cpp
)

PEERDIR(
    library/cpp/string_utils/quote
    library/cpp/threading/future
)

END()
