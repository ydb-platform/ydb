LIBRARY()

WITHOUT_LICENSE_TEXTS()

VERSION(2017-06-26-23eecfbe7e84ebf2e229bd02248f431c36e12f1a)

LICENSE(MIT)

NO_COMPILER_WARNINGS()

IF (NOT MSVC OR CLANG_CL)
    CFLAGS(-msse4.2)
ENDIF()

SRCDIR(contrib/libs/farmhash)

SRCS(
    farmhashsa.cc
    farmhashte.cc
)

END()
