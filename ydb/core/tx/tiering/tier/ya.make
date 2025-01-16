LIBRARY()

SRCS(
    object.cpp
    s3_uri.cpp
)

PEERDIR(
    ydb/library/conclusion
    ydb/services/metadata/secret/accessor
    contrib/restricted/aws/aws-crt-cpp
)

YQL_LAST_ABI_VERSION()

END()
