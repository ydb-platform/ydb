LIBRARY()

LICENSE(PSF-2.0)

NO_WSHADOW()

CFLAGS(
    GLOBAL -DARCADIA_PYTHON_UNICODE_SIZE=${ARCADIA_PYTHON_UNICODE_SIZE}
)

IF (NOT MSVC)
    CFLAGS(
        -fwrapv
    )
ELSE()
    # This is needed to properly build this module under MSVS IDE
    # The MsBuild doesn't have separate control over C and C++ flags,
    # so CXXFLAGS are now applied if there is .cpp file in the module.
    # Need to disable this to let C-code build
    SET_APPEND(CXXFLAGS /U_CRT_USE_BUILTIN_OFFSETOF)
ENDIF()

SRCDIR(
    contrib/tools/python/src/Include
)

INCLUDE(${ARCADIA_ROOT}/contrib/tools/python/pyconfig.inc)
INCLUDE(CMakeLists.inc)

CHECK_CONFIG_H(pyconfig.h)

END()
