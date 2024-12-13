LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    build_attributes.cpp
    config.cpp
    helpers.cpp
    program.cpp
    program_mixin.cpp
    program_config_mixin.cpp
    program_pdeathsig_mixin.cpp
    program_setsid_mixin.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/core/service_discovery/yp
    yt/yt/library/monitoring
    yt/yt/library/oom
    yt/yt/library/profiling/solomon
    yt/yt/library/profiling/tcmalloc
    yt/yt/library/profiling/perf
    yt/yt/library/stockpile
    yt/yt/library/ytprof
    yt/yt/library/tcmalloc
    yt/yt/library/tracing/jaeger
    library/cpp/yt/mlock
    library/cpp/yt/stockpile
    library/cpp/yt/string
    library/cpp/yt/backtrace/absl_unwinder
    library/cpp/getopt/small
)

END()
