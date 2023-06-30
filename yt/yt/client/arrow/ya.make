LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    fbs/Message.fbs
    fbs/Schema.fbs
    fbs/Tensor.fbs
    fbs/SparseTensor.fbs
    arrow_row_stream_encoder.cpp
    arrow_row_stream_decoder.cpp
    public.cpp
)

PEERDIR(
    yt/yt/client
    contrib/libs/flatbuffers
)

END()
