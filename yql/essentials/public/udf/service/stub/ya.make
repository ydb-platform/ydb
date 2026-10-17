YQL_LIBRARY()

PROVIDES(YqlServicePolicy)

SRCS(
    GLOBAL udf_service.cpp
)

PEERDIR(
    yql/essentials/public/udf
)

END()
