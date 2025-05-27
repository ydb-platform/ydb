PY3_PROGRAM(protoc-gen-mypy_grpc)

VERSION(Service-proxy-version)

LICENSE(Apache-2.0)

PEERDIR(
    contrib/python/mypy-protobuf
)

PY_MAIN(mypy_protobuf.main:grpc)

NO_LINT()

END()
