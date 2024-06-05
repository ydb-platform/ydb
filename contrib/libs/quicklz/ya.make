LIBRARY()

LICENSE(
    GPL-1.0-only OR
    GPL-2.0-only OR
    Quicklz
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(1.5.1)

NO_UTIL()

NO_COMPILER_WARNINGS()

IF (SANITIZER_TYPE == "undefined")
    NO_SANITIZE()
ENDIF()

ADDINCLSELF()

SRCS(
    all.c
    table.c
    quicklz.cpp
    1.51/tables.c
)

END()
