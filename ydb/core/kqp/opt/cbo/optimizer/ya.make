LIBRARY()

SRCS(
    cbo_optimizer_new.cpp
    cbo_hints.cpp
)

GENERATE_ENUM_SERIALIZATION(../cbo_optimizer_new.h)

PEERDIR(
    ydb/core/kqp/opt/cbo
    yql/essentials/core/statistics
    library/cpp/iterator
    yql/essentials/utils/log
)

END()
