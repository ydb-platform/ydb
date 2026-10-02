LIBRARY()

SRCS(
    regex_predicate.cpp
    resource_pool_classifier_settings.cpp
    resource_pool_settings.cpp
)

GENERATE_ENUM_SERIALIZATION(resource_pool_classifier_settings.h)

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
