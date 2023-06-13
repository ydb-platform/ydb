LIBRARY()

LICENSE(MIT)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(5.2.0)

PROVIDES(lua)

NO_COMPILER_WARNINGS()

IF (OS_LINUX)
    CFLAGS(-DLUA_USE_LINUX)
ELSEIF (OS_FREEBSD)
    CFLAGS(-DLUA_USE_LINUX)
ELSEIF (OS_DARWIN)
    CFLAGS(-DLUA_USE_MACOSX)
ELSEIF (OS_WINDOWS)
    CFLAGS(-DLUA_WIN)
ELSE()
    CFLAGS(
        -DLUA_USE_POSIX
        -DLUA_USE_DLOPEN
    )
ENDIF()

ADDINCL(contrib/libs/lua/lua-5.2.0/src)

SRCDIR(contrib/libs/lua/lua-5.2.0/src)

ARCHIVE(
    NAME common.inc
    common/stdlib.lua
    common/json.lua
)

PEERDIR(
    library/cpp/archive
)

SRCS(
    lib.cpp
)

END()
