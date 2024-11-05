LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    abortable_http_response.cpp
    context.cpp
    helpers.cpp
    host_manager.cpp
    http.cpp
    http_client.cpp
    requests.cpp
    retry_request.cpp
)

PEERDIR(
    library/cpp/deprecated/atomic
    library/cpp/http/io
    library/cpp/string_utils/base64
    library/cpp/string_utils/quote
    library/cpp/threading/cron
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/interface/logging
    yt/yt/core/http
    yt/yt/core/https
)

END()

RECURSE_FOR_TESTS(
    ut
)
