LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    auth.cpp
    authentication_options.cpp
    credentials_injecting_channel.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/library/tvm
)

END()

RECURSE_FOR_TESTS(
    unittests
)
