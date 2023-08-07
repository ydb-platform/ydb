LIBRARY()

SRCS(
    udf_support.cpp
)

PEERDIR(
    ydb/library/yql/public/udf
)

PROVIDES(YqlUdfSdkSupport)

YQL_LAST_ABI_VERSION()

END()
