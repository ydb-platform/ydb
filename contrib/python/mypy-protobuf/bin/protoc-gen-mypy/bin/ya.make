PY3_PROGRAM_BIN(protoc-gen-mypy)

LICENSE(Apache-2.0)

PEERDIR(
    contrib/python/mypy-protobuf
)

PY_MAIN(mypy_protobuf.main:main)

NO_LINT()

END()
