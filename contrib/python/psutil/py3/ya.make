PY3_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(5.8.0)

NO_UTIL()

SRCDIR(contrib/python/psutil/py3/psutil)

NO_COMPILER_WARNINGS()

CFLAGS(
    -DPSUTIL_VERSION=580
)

IF (OS_LINUX OR OS_DARWIN)
    CFLAGS(
        -DPSUTIL_POSIX=1
    )
    SRCS(
        _psutil_common.c
        _psutil_posix.c
    )
    PY_REGISTER(psutil._psutil_posix)
ENDIF ()

IF (OS_LINUX)
    CFLAGS(
        -DPSUTIL_LINUX=1
    )

    SRCS(
        _psutil_linux.c
    )
    PY_REGISTER(psutil._psutil_linux)
ENDIF ()

IF (OS_DARWIN)
    CFLAGS(
        -DPSUTIL_OSX=1
    )

    EXTRALIBS("-framework CoreFoundation -framework IOKit")

    PEERDIR(
        contrib/python/psutil/py3/psutil/arch/osx
    )

    SRCS(
        _psutil_osx.c
    )

    PY_REGISTER(psutil._psutil_osx)
ENDIF ()

IF (OS_WINDOWS)
    CFLAGS(
        -DPSUTIL_WINDOWS=1
        -DPSUTIL_SIZEOF_PID_T=4
    )

    LDFLAGS(
        Shell32.lib
        PowrProf.lib
        Wtsapi32.lib
        Pdh.lib
    )

    SRCS(
        _psutil_common.c
        _psutil_windows.c
        arch/windows/cpu.c
        arch/windows/disk.c
        arch/windows/net.c
        arch/windows/process_handles.c
        arch/windows/process_info.c
        arch/windows/process_utils.c
        arch/windows/security.c
        arch/windows/services.c
        arch/windows/socks.c
        arch/windows/wmi.c
    )

    PY_REGISTER(psutil._psutil_windows)
ENDIF ()

NO_CHECK_IMPORTS(
    psutil._psbsd
    psutil._psosx
    psutil._pssunos
    psutil._psutil_bsd
    psutil._psutil_common
    psutil._psutil_osx
    psutil._psutil_sunos
    psutil._psutil_windows
    psutil._pswindows
)

PY_SRCS(
    TOP_LEVEL
    psutil/__init__.py
    psutil/_common.py
    psutil/_compat.py
)

IF (OS_LINUX OR OS_DARWIN)
    PY_SRCS(
        TOP_LEVEL
        psutil/_psposix.py
    )
ENDIF ()

IF (OS_LINUX)
    PY_SRCS(
        TOP_LEVEL
        psutil/_pslinux.py
    )
ENDIF ()

IF (OS_DARWIN)
    PY_SRCS(
        TOP_LEVEL
        psutil/_psosx.py
    )
ENDIF ()

IF (OS_WINDOWS)
    PY_SRCS(
        TOP_LEVEL
        psutil/_pswindows.py
    )
ENDIF ()

RESOURCE_FILES(
    PREFIX contrib/python/psutil/py3/
    .dist-info/METADATA
    .dist-info/top_level.txt
)

NO_LINT()

END()

RECURSE_FOR_TESTS(
    test
)
