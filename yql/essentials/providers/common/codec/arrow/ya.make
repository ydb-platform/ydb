LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    yql/essentials/providers/common/codec
    yql/essentials/public/udf/arrow
)

SRCS(
    yql_codec_buf_input_stream.cpp
    yql_codec_buf_output_stream.cpp
)

END()
