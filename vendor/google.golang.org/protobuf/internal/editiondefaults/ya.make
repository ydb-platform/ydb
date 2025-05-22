GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v1.36.5)

SRCS(
    defaults.go
)

GO_EMBED_PATTERN(editions_defaults.binpb)

END()
