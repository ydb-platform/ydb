LIBRARY()

SRCS(
    common_app.cpp
)



PEERDIR(
    library/cpp/monlib/service/pages
)

END()

RECURSE_FOR_TESTS(
#    ut
#    dread_cache_service/ut
#    ut/slow
#    ut/ut_with_sdk
)
