PY2_LIBRARY()

LICENSE(Apache-2.0)

VERSION(0.20.0)

PEERDIR(
    contrib/python/six
)

ADDINCL(
    contrib/python/thrift/py2/thrift
)

NO_LINT()

NO_COMPILER_WARNINGS()

NO_CHECK_IMPORTS(
    thrift.TSCons
    thrift.TTornado
    thrift.transport.TTwisted
)

IF (NOT OS_WINDOWS)
    SRCS(
        thrift/ext/binary.cpp
        thrift/ext/compact.cpp
        thrift/ext/module.cpp
        thrift/ext/types.cpp
    )

    PY_REGISTER(
        fastbinary
    )
ENDIF()

PY_SRCS(
    TOP_LEVEL
    thrift/TMultiplexedProcessor.py
    thrift/TRecursive.py
    thrift/TSCons.py
    thrift/TSerialization.py
    thrift/TTornado.py
    thrift/Thrift.py
    thrift/__init__.py
    thrift/compat.py
    thrift/protocol/TBase.py
    thrift/protocol/TBinaryProtocol.py
    thrift/protocol/TCompactProtocol.py
    thrift/protocol/THeaderProtocol.py
    thrift/protocol/TJSONProtocol.py
    thrift/protocol/TMultiplexedProtocol.py
    thrift/protocol/TProtocol.py
    thrift/protocol/TProtocolDecorator.py
    thrift/protocol/__init__.py
    thrift/server/THttpServer.py
    thrift/server/TNonblockingServer.py
    thrift/server/TProcessPoolServer.py
    thrift/server/TServer.py
    thrift/server/__init__.py
    thrift/transport/THeaderTransport.py
    thrift/transport/THttpClient.py
    thrift/transport/TSSLSocket.py
    thrift/transport/TSocket.py
    thrift/transport/TTransport.py
    thrift/transport/TTwisted.py
    thrift/transport/TZlibTransport.py
    thrift/transport/__init__.py
    thrift/transport/sslcompat.py
)

RESOURCE_FILES(
    PREFIX contrib/python/thrift/py2/
    .dist-info/METADATA
    .dist-info/top_level.txt
)

END()

RECURSE_FOR_TESTS(
    tests
)
