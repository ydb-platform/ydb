LIBRARY()

LICENSE(GPL-2.0-or-later)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(2.09)

CFLAGS(-DMINILZO_CFG_SKIP_LZO_STRING)

NO_UTIL()

NO_COMPILER_WARNINGS()

IF (SANITIZER_TYPE == "undefined")
    NO_SANITIZE()
ENDIF()

SRCS(
    minilzo.c
    lzo_crc.c
)

END()
