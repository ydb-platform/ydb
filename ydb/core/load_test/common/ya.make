LIBRARY()

SRCS(
    ../defs.h
    ../events.cpp
    ../events.h
    ../gen.h
    ../interval_gen.h
    ../memory.cpp
    ../percentile.h
    ../quantile.h
    ../service_actor.h
    ../size_gen.h
    ../speed.h
    ../time_series.h
    ../util.cpp
    ../util.h
)

PEERDIR(
    library/cpp/histogram/hdr
    library/cpp/json/writer
    library/cpp/monlib/dynamic_counters
    library/cpp/monlib/dynamic_counters/percentile
    library/cpp/monlib/service/pages
    library/cpp/random_provider
    library/cpp/time_provider
    ydb/core/base
    ydb/core/protos
    ydb/library/actors/core
    ydb/library/services
)

GENERATE_ENUM_SERIALIZATION(../percentile.h)

END()
