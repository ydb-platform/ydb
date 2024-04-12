GO_LIBRARY()
IF (TRUE)
    SRCS(
        bits.go
        block.go
        fse.go
        huff.go
        literals.go
        window.go
        xxhash.go
        zstd.go
    )
ENDIF()
END()
