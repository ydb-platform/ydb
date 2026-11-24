YQL_LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    yql/essentials/public/udf
    yql/essentials/utils
    yql/essentials/public/udf/arrow
)

SRCS(
    yt_arrow_converter.cpp
    yt_arrow_converter_details.cpp
)

PEERDIR_FROM_POLICY(
    ALLOW
    quality/user_sessions/createlib/qb3/parser
    transfer_manager/go/cmd/yql/yql_serverd
)

END()
