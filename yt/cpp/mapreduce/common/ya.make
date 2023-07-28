LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    debug_metrics.cpp
    helpers.cpp
    retry_lib.cpp
    wait_proxy.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/svnversion
    library/cpp/threading/future
    library/cpp/yson
    library/cpp/yson/json
    library/cpp/yson/node
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/interface/logging
)

END()
