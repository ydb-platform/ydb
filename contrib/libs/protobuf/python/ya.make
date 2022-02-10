PACKAGE()

WITHOUT_LICENSE_TEXTS()

LICENSE(BSD-3-Clause) 
 
OWNER(
    g:contrib
    g:cpp-contrib
)

GENERATE_PY_PROTOS(contrib/libs/protobuf/src/google/protobuf/descriptor.proto)

END()
