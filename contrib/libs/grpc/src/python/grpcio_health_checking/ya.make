PY23_LIBRARY()

LICENSE(Apache-2.0) 
 
LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

OWNER(
    akastornov
    dvshkurko
    g:contrib
    g:cpp-contrib
)

PEERDIR(
    contrib/libs/grpc/grpc
    contrib/python/six
)

IF (PYTHON2)
    PEERDIR(
        contrib/python/enum34
        contrib/python/futures
    )
ENDIF()

ADDINCL(
    ${ARCADIA_BUILD_ROOT}/contrib/libs/grpc
    contrib/libs/grpc
    contrib/libs/grpc/include
)

IF (SANITIZER_TYPE == undefined)
    # https://github.com/grpc/grpc/blob/v1.15.1/tools/bazel.rc#L43
    CXXFLAGS(-fno-sanitize=function)
ENDIF()

NO_LINT()

NO_COMPILER_WARNINGS()

PY_SRCS(
    TOP_LEVEL
    grpc_health/__init__.py
    grpc_health/v1/__init__.py
    grpc_health/v1/health.py
)

END()
