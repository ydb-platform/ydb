LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    backend.cpp
)

PEERDIR(
    library/cpp/yt/assert
    library/cpp/yt/logging

    library/cpp/logger
)

END()
