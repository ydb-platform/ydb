LIBRARY()

WITHOUT_LICENSE_TEXTS()

# Proxy library
LICENSE(Not-Applicable)

NO_PLATFORM()

IF (NOT USE_STL_SYSTEM)
    IF (MSVC AND NOT CLANG_CL)
        PEERDIR(
            contrib/libs/cxxsupp/libcxxmsvc
        )
    ELSE()
        PEERDIR(
            contrib/libs/cxxsupp/libcxx
        )
    ENDIF()
ELSE()
    PEERDIR(
        contrib/libs/cxxsupp/system_stl
    )
ENDIF()

END()

RECURSE(
    libcxx
    libcxxabi
    libcxxmsvc
    libcxxrt
    openmp
)
