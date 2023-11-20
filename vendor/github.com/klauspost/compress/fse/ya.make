GO_LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause AND
    MIT
)

SRCS(
    bitreader.go
    bitwriter.go
    bytereader.go
    compress.go
    decompress.go
    fse.go
)

GO_TEST_SRCS(fse_test.go)

END()

RECURSE(gotest)
