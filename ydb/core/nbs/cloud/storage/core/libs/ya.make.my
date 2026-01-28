LIBRARY()

GENERATE_ENUM_SERIALIZATION(common/error.h)

SRCS(
    common/context.cpp
    common/error.cpp
    common/helpers.cpp
    common/page_size.cpp
    common/startable.cpp
    common/thread.cpp

    diagnostics/histogram.cpp
    diagnostics/logging.cpp
)

PEERDIR(
    library/cpp/lwtrace
    util
    ydb/core/protos/nbs
    ydb/library/services
)

END()
