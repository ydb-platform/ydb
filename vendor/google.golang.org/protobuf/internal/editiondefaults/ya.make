GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    defaults.go
)

GO_EMBED_PATTERN(editions_defaults.binpb)

END()
