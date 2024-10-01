LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    driver.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/driver
)

END()
