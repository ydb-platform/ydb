LIBRARY()

SRCS(
    Message.fbs
    Schema.fbs
    Tensor.fbs
    SparseTensor.fbs
)

PEERDIR(
    contrib/libs/flatbuffers
)

END()
