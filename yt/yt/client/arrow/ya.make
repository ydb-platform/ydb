LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    arrow_row_stream_encoder.cpp
    arrow_row_stream_decoder.cpp
    public.cpp
)

PEERDIR(
    yt/yt/client
    yt/yt/client/arrow/fbs
)

END()
