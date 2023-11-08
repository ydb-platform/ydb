GO_LIBRARY()

RESOURCE(
    certs/cacert.pem /certifi/common.pem
    certs/yandex_internal.pem /certifi/internal.pem
)

SRCS(certs.go)

END()
