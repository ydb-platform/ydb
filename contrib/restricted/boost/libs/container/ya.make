LIBRARY()

LICENSE(
    BSL-1.0 AND
    CC0-1.0
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

OWNER(
    antoshkka
    g:cpp-committee
    g:cpp-contrib
)

INCLUDE(${ARCADIA_ROOT}/contrib/restricted/boost/boost_common.inc)

SRCS(
    src/alloc_lib.c
    src/dlmalloc.cpp
    src/global_resource.cpp
    src/monotonic_buffer_resource.cpp
    src/pool_resource.cpp
    src/synchronized_pool_resource.cpp
    src/unsynchronized_pool_resource.cpp
)

END()
