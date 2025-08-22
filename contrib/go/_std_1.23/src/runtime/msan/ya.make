IF (CGO_ENABLED)
GO_LIBRARY()

    PEERDIR(contrib/libs/clang${COMPILER_VERSION}-rt/lib/msan)
    CGO_SRCS(msan.go)

END()
ENDIF()
