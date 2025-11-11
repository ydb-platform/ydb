PY2_LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause AND
    MPL-2.0 AND
    Python-2.0
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(1.50.0)

ORIGINAL_SOURCE(mirror://pypi/g/grpcio/grpcio-1.50.0.tar.gz)

PEERDIR(
    contrib/libs/grpc
    contrib/python/six
)

PEERDIR(
    contrib/deprecated/python/enum34
    contrib/deprecated/python/futures
)

ADDINCL(
    ${ARCADIA_BUILD_ROOT}/contrib/libs/grpc
    contrib/libs/grpc
    contrib/libs/grpc/include
    FOR
    cython
    contrib/python/grpcio/py2
)

IF (SANITIZER_TYPE == undefined)
    CXXFLAGS(-fno-sanitize=function)
ENDIF()

NO_COMPILER_WARNINGS()

NO_LINT()

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

END()
