GO_PROGRAM()

LICENSE(Apache-2.0)

SRCS(
    main.go
)

GO_EMBED_PATTERN(infer.go.tmpl)

GO_EMBED_PATTERN(main.go.tmpl)

GO_EMBED_PATTERN(safe.go.tmpl)

GO_EMBED_PATTERN(test.go.tmpl)

GO_EMBED_PATTERN(unsafe.go.tmpl)

END()
