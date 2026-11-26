YQL_LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    yql/essentials/minikql/arrow
    yql/essentials/providers/common/codec
    yql/essentials/public/udf/arrow
)

SRCS(
    yql_codec_buf_input_stream.cpp
    yql_codec_buf_output_stream.cpp
)

PEERDIR_FROM_POLICY(
    ALLOW
    quality/user_sessions/createlib/qb3/parser
    transfer_manager/go/cmd/yql/yql_serverd
)

END()
