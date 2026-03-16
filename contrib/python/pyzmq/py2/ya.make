PY2_LIBRARY()

LICENSE(GPL-3.0-or-later)

VERSION(19.0.2)

NO_COMPILER_WARNINGS()

PEERDIR(
    contrib/libs/zeromq
)

ADDINCL(
    FOR cython contrib/python/pyzmq/py2
    contrib/python/pyzmq/py2/zmq/utils
)

IF(OS_WINDOWS)
    SET(PLATFORM_SRCS zmq/eventloop/minitornado/platform/windows.py)
ELSE()
    SET(PLATFORM_SRCS zmq/eventloop/minitornado/platform/posix.py)
ENDIF()

PY_SRCS(
    TOP_LEVEL
    zmq/__init__.py
    zmq/_future.py
    zmq/auth/__init__.py
    zmq/auth/base.py
    zmq/auth/certs.py
    zmq/auth/ioloop.py
    zmq/auth/thread.py
    zmq/backend/__init__.py
    #zmq/backend/cffi/__init__.py
    #zmq/backend/cffi/_cffi.py
    #zmq/backend/cffi/_poll.py
    #zmq/backend/cffi/constants.py
    #zmq/backend/cffi/context.py
    #zmq/backend/cffi/devices.py
    #zmq/backend/cffi/error.py
    #zmq/backend/cffi/message.py
    #zmq/backend/cffi/socket.py
    #zmq/backend/cffi/utils.py
    zmq/backend/cython/__init__.py
    zmq/backend/select.py
    zmq/decorators.py
    zmq/devices/__init__.py
    zmq/devices/basedevice.py
    zmq/devices/monitoredqueue.py
    zmq/devices/monitoredqueuedevice.py
    zmq/devices/proxydevice.py
    zmq/devices/proxysteerabledevice.py
    zmq/error.py
    zmq/eventloop/__init__.py
    zmq/eventloop/_deprecated.py
    zmq/eventloop/future.py
    zmq/eventloop/ioloop.py
    zmq/eventloop/minitornado/__init__.py
    zmq/eventloop/minitornado/concurrent.py
    zmq/eventloop/minitornado/ioloop.py
    zmq/eventloop/minitornado/log.py
    zmq/eventloop/minitornado/platform/__init__.py
    zmq/eventloop/minitornado/platform/auto.py
    zmq/eventloop/minitornado/platform/common.py
    zmq/eventloop/minitornado/platform/interface.py
    zmq/eventloop/minitornado/stack_context.py
    zmq/eventloop/minitornado/util.py
    zmq/eventloop/zmqstream.py
    zmq/green/__init__.py
    zmq/green/core.py
    zmq/green/device.py
    zmq/green/eventloop/__init__.py
    zmq/green/eventloop/ioloop.py
    zmq/green/eventloop/zmqstream.py
    zmq/green/poll.py
    zmq/log/__init__.py
    zmq/log/__main__.py
    zmq/log/handlers.py
    zmq/ssh/__init__.py
    zmq/ssh/forward.py
    zmq/ssh/tunnel.py
    zmq/sugar/__init__.py
    zmq/sugar/attrsettr.py
    zmq/sugar/constants.py
    zmq/sugar/context.py
    zmq/sugar/frame.py
    zmq/sugar/poll.py
    zmq/sugar/socket.py
    zmq/sugar/stopwatch.py
    zmq/sugar/tracker.py
    zmq/sugar/version.py
    zmq/tests/__init__.py
    zmq/utils/__init__.py
    zmq/utils/constant_names.py
    zmq/utils/garbage.py
    zmq/utils/interop.py
    zmq/utils/jsonapi.py
    zmq/utils/monitor.py
    zmq/utils/sixcerpt.py
    zmq/utils/strtypes.py
    zmq/utils/win32.py
    zmq/utils/z85.py

    ${PLATFORM_SRCS}

    CYTHON_C
    zmq/backend/cython/_device.pyx
    zmq/backend/cython/_poll.pyx
    zmq/backend/cython/_proxy_steerable.pyx
    zmq/backend/cython/_version.pyx
    zmq/backend/cython/constants.pyx
    zmq/backend/cython/context.pyx
    zmq/backend/cython/error.pyx
    zmq/backend/cython/message.pyx
    zmq/backend/cython/rebuffer.pyx
    zmq/backend/cython/socket.pyx
    zmq/backend/cython/utils.pyx
    zmq/devices/monitoredqueue.pyx
)

NO_LINT()
NO_CHECK_IMPORTS(
    zmq.auth.ioloop
    zmq.eventloop.future
    zmq.green.*
    zmq.tests.*
)

RESOURCE_FILES(
    PREFIX contrib/python/pyzmq/py2/
    .dist-info/METADATA
    .dist-info/top_level.txt
)

END()

RECURSE_FOR_TESTS(
    tests
)
