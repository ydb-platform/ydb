GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v1.36.12-0.20260120151049-f2248ac996af)

SRCS(
    defaults.go
)

GO_EMBED_PATTERN(editions_defaults.binpb)

END()
