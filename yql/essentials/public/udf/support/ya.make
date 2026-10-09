YQL_LIBRARY()

SRCS(
    udf_support.cpp
)

PEERDIR(
    yql/essentials/public/udf
)

PROVIDES(YqlUdfSdkSupport)

END()
