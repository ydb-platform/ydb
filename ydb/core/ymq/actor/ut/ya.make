UNITTEST()

PEERDIR(
    contrib/libs/yaml-cpp
    ydb/core/mind/address_classification
    ydb/core/testlib/default
    ydb/core/ymq/actor
    ydb/core/ymq/base
    ydb/core/ymq/http
)

SRCS(
    attributes_md5_ut.cpp
    infly_ut.cpp
    message_delay_stats_ut.cpp
    sha256_ut.cpp
    metering_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
