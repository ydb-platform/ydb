GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    block.go
    frame.go
    frame_gen.go
)

GO_TEST_SRCS(frame_test.go)

END()

RECURSE(gotest)
