PACKAGE()

WITHOUT_LICENSE_TEXTS()

VERSION(Service-proxy-version)

LICENSE(BSD-3-Clause)

GENERATE_PY_PROTOS(contrib/libs/protobuf/src/google/protobuf/descriptor.proto)

END()
