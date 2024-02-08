GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    desc.go
    desc_init.go
    desc_resolve.go
    desc_validate.go
    editions.go
    proto.go
)

GO_EMBED_PATTERN(editions_defaults.binpb)

END()
