LIBRARY()

SRCS(
    common_app.cpp
)



PEERDIR(
    library/cpp/monlib/service/pages
    ydb/core/persqueue/public
)

END()

RECURSE_FOR_TESTS(
)
