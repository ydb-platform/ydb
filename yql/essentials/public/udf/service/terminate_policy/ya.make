YQL_LIBRARY()

PROVIDES(YqlServicePolicy)

IF (MKQL_DISABLE_UDF_SERVICE_DISPATCH)
    SRCS(
        GLOBAL udf_service_direct.cpp
    )
    PEERDIR(
        yql/essentials/minikql
    )
ELSE()
    SRCS(
        GLOBAL udf_service.cpp
    )
    PEERDIR(
        yql/essentials/public/udf/service/dispatch
    )
ENDIF()

PEERDIR(
    yql/essentials/public/udf
)

END()
