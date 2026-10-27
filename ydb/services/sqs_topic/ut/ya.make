UNITTEST()

SRCS(
    receipt_ut.cpp
    utils_ut.cpp
)

PEERDIR(
    library/cpp/cgiparam
    library/cpp/getopt
    library/cpp/json
    library/cpp/string_utils/quote
    library/cpp/svnversion
    ydb/core/http_proxy
    ydb/core/testlib/default
    ydb/services/sqs_topic
)

YQL_LAST_ABI_VERSION()

END()
