LIBRARY()

SRCS(
    object.cpp
)

PEERDIR(
    ydb/library/conclusion
    ydb/services/metadata/secret/accessor
)

YQL_LAST_ABI_VERSION()

END()
