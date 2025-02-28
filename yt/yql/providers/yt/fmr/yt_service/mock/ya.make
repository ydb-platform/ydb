LIBRARY()

SRCS(
    yql_yt_yt_service_mock.h
)

PEERDIR(
    yt/yql/providers/yt/fmr/yt_service/interface
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
