GO_LIBRARY()

SRCS(
    ar.go
    gccgoinstallation.go
    importer.go
    parser.go
)

GO_TEST_SRCS(
    gccgoinstallation_test.go
    importer_test.go
    parser_test.go
)

END()

RECURSE(
)
