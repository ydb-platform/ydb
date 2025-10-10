LIBRARY()

SRCS(
    udf_support.cpp
)

PEERDIR(
    yql/essentials/public/udf
)

PROVIDES(YqlUdfSdkSupport)

YQL_LAST_ABI_VERSION()

END()
