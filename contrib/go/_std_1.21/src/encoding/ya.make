GO_LIBRARY()

SRCS(
    encoding.go
)

END()

RECURSE(
    ascii85
    asn1
    base32
    base64
    binary
    csv
    gob
    hex
    json
    pem
    xml
)
