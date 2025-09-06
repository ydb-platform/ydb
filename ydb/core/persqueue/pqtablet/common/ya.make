LIBRARY()

SRCS(
)



PEERDIR(
    ydb/core/base
    ydb/library/logger
)

END()

RECURSE_FOR_TESTS(
#    ut
#    dread_cache_service/ut
#    ut/slow
#    ut/ut_with_sdk
)
