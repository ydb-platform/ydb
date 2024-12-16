LIBRARY()

WITHOUT_LICENSE_TEXTS()

VERSION(Service-proxy-version)

LICENSE(YandexOpen)

# Placeholders for new GCC 4.9.2 C++ ABI which is not present on older systems

SUBSCRIBER(
    somov
    g:contrib
    g:cpp-contrib
)

IF (NOT OS_WINDOWS)
    SRCS(
        cxxabi.cpp
        stdcxx_bits.cpp
    )
ENDIF()

NO_UTIL()

END()
