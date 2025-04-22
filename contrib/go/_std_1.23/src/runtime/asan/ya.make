IF (CGO_ENABLED)
GO_LIBRARY()

    PEERDIR(contrib/libs/clang${COMPILER_VERSION}-rt/lib/asan)
    CGO_SRCS(asan.go)

END()
ENDIF()
