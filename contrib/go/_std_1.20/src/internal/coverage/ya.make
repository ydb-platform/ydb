GO_LIBRARY()

SRCS(
    cmddefs.go
    defs.go
    pkid.go
)

END()

RECURSE(
    calloc
    cformat
    cmerge
    decodecounter
    decodemeta
    encodecounter
    encodemeta
    pods
    rtcov
    slicereader
    slicewriter
    stringtab
    # test
    uleb128
)
