GO_LIBRARY()

SRCS(
    doc.go
    goboringcrypto.h
    notboring.go
)

END()

RECURSE(
    bbig
    bcache
    sig
)
