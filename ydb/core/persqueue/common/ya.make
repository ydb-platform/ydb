LIBRARY()

SRCS(
    common_app.cpp
    microseconds_sliding_window.cpp
)



PEERDIR(
    library/cpp/monlib/service/pages
    ydb/core/persqueue/public
)

END()

RECURSE_FOR_TESTS(
    ut
)
