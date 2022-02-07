LIBRARY()

LICENSE(BSL-1.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

OWNER(
    antoshkka
    g:cpp-committee
    g:cpp-contrib
)

INCLUDE(${ARCADIA_ROOT}/contrib/restricted/boost/boost_common.inc)

PEERDIR(
    contrib/libs/zlib
    contrib/libs/libbz2
)

CFLAGS(
    -DBOOST_IOSTREAMS_USE_DEPRECATED
)

SRCS(
    src/file_descriptor.cpp
    src/gzip.cpp
    src/mapped_file.cpp
    src/zlib.cpp
)

END()
