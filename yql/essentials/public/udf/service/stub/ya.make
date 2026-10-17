YQL_LIBRARY()

PROVIDES(YqlServicePolicy)
# gn: default provider

SRCS(
    GLOBAL udf_service.cpp
)

PEERDIR(
    yql/essentials/public/udf
)

END()
