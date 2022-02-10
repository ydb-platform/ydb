PY23_LIBRARY()

WITHOUT_LICENSE_TEXTS()

LICENSE(Apache-2.0)

OWNER(
    akastornov
    g:contrib
    g:cpp-contrib
)

PEERDIR(
    contrib/libs/grpc/src/python/grpcio
)

END()

RECURSE_ROOT_RELATIVE(
    contrib/libs/grpc/src/python/grpcio
)
