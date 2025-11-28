LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/yt/library/tracing
)

SRCS(
    baggage_manager.cpp
    config.cpp
    GLOBAL configure_baggage_manager.cpp
)

END()
