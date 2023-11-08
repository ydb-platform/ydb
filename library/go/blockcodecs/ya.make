GO_LIBRARY()

SRCS(
    codecs.go
    decoder.go
    encoder.go
    nop_codec.go
)

END()

RECURSE(
    all
    blockbrotli
    blocklz4
    blocksnappy
    blockzstd
    integration
)
