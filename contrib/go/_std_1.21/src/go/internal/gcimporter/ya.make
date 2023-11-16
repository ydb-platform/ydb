GO_LIBRARY()

SRCS(
    exportdata.go
    gcimporter.go
    iimport.go
    support.go
    ureader.go
)

GO_XTEST_SRCS(gcimporter_test.go)

END()

RECURSE(
)
