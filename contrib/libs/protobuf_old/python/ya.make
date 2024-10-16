PACKAGE()

WITHOUT_LICENSE_TEXTS()

LICENSE(BSD-3-Clause)

SUBSCRIBER(
    g:contrib
    g:cpp-contrib
)

GENERATE_PY_PROTOS(contrib/libs/protobuf_old/src/google/protobuf/descriptor.proto)

END()
