LIBRARY()

VERSION(0.10.0)

LICENSE(Apache-2.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

NO_UTIL()

NO_COMPILER_WARNINGS()

ADDINCL(
    GLOBAL contrib/restricted/thrift
)

PEERDIR(
    contrib/libs/libevent
    contrib/libs/openssl
    contrib/libs/zlib
    contrib/restricted/boost/interprocess
    contrib/restricted/boost/locale
    contrib/restricted/boost/math
    contrib/restricted/boost/system
    contrib/restricted/boost/thread
)

SRCS(
    thrift/TApplicationException.cpp
    thrift/TOutput.cpp
    thrift/VirtualProfiling.cpp
    thrift/async/TAsyncChannel.cpp
    thrift/async/TAsyncProtocolProcessor.cpp
    thrift/async/TConcurrentClientSyncInfo.cpp
    thrift/async/TEvhttpClientChannel.cpp
    thrift/async/TEvhttpServer.cpp
    thrift/concurrency/StdMonitor.cpp
    thrift/concurrency/StdMutex.cpp
    thrift/concurrency/StdThreadFactory.cpp
    thrift/concurrency/ThreadManager.cpp
    thrift/concurrency/TimerManager.cpp
    thrift/concurrency/Util.cpp
    thrift/processor/PeekProcessor.cpp
    thrift/protocol/TBase64Utils.cpp
    thrift/protocol/TDebugProtocol.cpp
    thrift/protocol/THeaderProtocol.cpp
    thrift/protocol/TJSONProtocol.cpp
    thrift/protocol/TMultiplexedProtocol.cpp
    thrift/protocol/TProtocol.cpp
    thrift/server/TConnectedClient.cpp
    thrift/server/TNonblockingServer.cpp
    thrift/server/TServer.cpp
    thrift/server/TServerFramework.cpp
    thrift/server/TSimpleServer.cpp
    thrift/server/TThreadPoolServer.cpp
    thrift/server/TThreadedServer.cpp
    thrift/transport/TBufferTransports.cpp
    thrift/transport/TFDTransport.cpp
    thrift/transport/TFileTransport.cpp
    thrift/transport/THeaderTransport.cpp
    thrift/transport/THttpClient.cpp
    thrift/transport/THttpServer.cpp
    thrift/transport/THttpTransport.cpp
    thrift/transport/TNonblockingSSLServerSocket.cpp
    thrift/transport/TNonblockingServerSocket.cpp
    thrift/transport/TPipe.cpp
    thrift/transport/TPipeServer.cpp
    thrift/transport/TSSLServerSocket.cpp
    thrift/transport/TSSLSocket.cpp
    thrift/transport/TServerSocket.cpp
    thrift/transport/TSimpleFileTransport.cpp
    thrift/transport/TSocket.cpp
    thrift/transport/TSocketPool.cpp
    thrift/transport/TTransportException.cpp
    thrift/transport/TTransportUtils.cpp
    thrift/transport/TZlibTransport.cpp
)

IF (OS_WINDOWS)
    PEERDIR(
        contrib/restricted/boost/scope_exit
    )
    SRCS(
        thrift/windows/GetTimeOfDay.cpp
        thrift/windows/OverlappedSubmissionThread.cpp
        thrift/windows/SocketPair.cpp
        thrift/windows/TWinsockSingleton.cpp
        thrift/windows/WinFcntl.cpp
    )
ELSE()
    CXXFLAGS(
        -Wno-deprecated-declarations
        -Wno-unused-function
        -Wno-unused-parameter
        -Wno-unused-private-field
        -Wno-unused-variable
    )
ENDIF()

END()
