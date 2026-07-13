LIBRARY()

SRCS(
    regex_predicate.cpp
    resource_pool_classifier_settings.cpp
    resource_pool_settings.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/regex/pcre
    util
    ydb/library/aclib
)

END()

RECURSE_FOR_TESTS(
    ut
)
