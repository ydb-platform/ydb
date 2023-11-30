LIBRARY()

LICENSE(BSD-3-Clause)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

NO_UTIL()

NO_COMPILER_WARNINGS()

IF (OS_DARWIN)
    PEERDIR(
        contrib/libs/gperftools
    )
ELSE()
    SRCS(
        galloc.cpp
        hack.cpp
    )
ENDIF()

END()
