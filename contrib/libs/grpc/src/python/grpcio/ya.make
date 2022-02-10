PY23_LIBRARY()

LICENSE(Apache-2.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

OWNER(
    akastornov 
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
    grpc/__init__.py
    grpc/_auth.py
    grpc/_channel.py
    grpc/_common.py
    grpc/_compression.py 
    grpc/_cython/__init__.py
    grpc/_cython/_cygrpc/__init__.py
    grpc/_cython/cygrpc.pyx
    grpc/_grpcio_metadata.py
    grpc/_interceptor.py
    grpc/_plugin_wrapping.py
    grpc/_runtime_protos.py
    grpc/_server.py
    grpc/_utilities.py
    grpc/beta/__init__.py
    grpc/beta/_client_adaptations.py
    grpc/beta/_metadata.py
    grpc/beta/_server_adaptations.py
    grpc/beta/implementations.py
    grpc/beta/interfaces.py
    grpc/beta/utilities.py
    grpc/experimental/__init__.py
    grpc/experimental/gevent.py
    grpc/experimental/session_cache.py
    grpc/framework/__init__.py
    grpc/framework/common/__init__.py
    grpc/framework/common/cardinality.py
    grpc/framework/common/style.py
    grpc/framework/foundation/__init__.py
    grpc/framework/foundation/abandonment.py
    grpc/framework/foundation/callable_util.py
    grpc/framework/foundation/future.py
    grpc/framework/foundation/logging_pool.py
    grpc/framework/foundation/stream.py
    grpc/framework/foundation/stream_util.py
    grpc/framework/interfaces/__init__.py
    grpc/framework/interfaces/base/__init__.py
    grpc/framework/interfaces/base/base.py
    grpc/framework/interfaces/base/utilities.py
    grpc/framework/interfaces/face/__init__.py
    grpc/framework/interfaces/face/face.py
    grpc/framework/interfaces/face/utilities.py
)

IF (PYTHON3) 
    PY_SRCS( 
        TOP_LEVEL 
        grpc/_simple_stubs.py
        grpc/aio/_base_call.py
        grpc/aio/_base_channel.py
        grpc/aio/_base_server.py
        grpc/aio/_call.py
        grpc/aio/_channel.py
        grpc/aio/__init__.py
        grpc/aio/_interceptor.py
        grpc/aio/_metadata.py
        grpc/aio/_server.py
        grpc/aio/_typing.py
        grpc/aio/_utils.py
        grpc/experimental/aio/__init__.py 
    ) 
ENDIF() 
 
END()
