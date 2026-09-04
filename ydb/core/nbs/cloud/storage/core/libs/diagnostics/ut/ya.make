UNITTEST_FOR(ydb/core/nbs/cloud/storage/core/libs/diagnostics)

SRCDIR(ydb/core/nbs/cloud/storage/core/libs/diagnostics)

PEERDIR(
    library/cpp/json
)

SRCS(
    histogram_types_ut.cpp
    logging_ut.cpp
    max_calculator_ut.cpp
    postpone_time_predictor_ut.cpp
    request_counters_ut.cpp
    weighted_percentile_ut.cpp
)

END()
