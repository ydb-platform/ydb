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
    contrib/libs/icu
)

CFLAGS(
    -DBOOST_HAS_ICU
)

SRCS(
    src/c_regex_traits.cpp
    src/cpp_regex_traits.cpp
    src/cregex.cpp
    src/fileiter.cpp
    src/icu.cpp
    src/instances.cpp
    src/posix_api.cpp
    src/regex.cpp
    src/regex_debug.cpp
    src/regex_raw_buffer.cpp
    src/regex_traits_defaults.cpp
    src/static_mutex.cpp
    src/usinstances.cpp
    src/w32_regex_traits.cpp
    src/wc_regex_traits.cpp
    src/wide_posix_api.cpp
    src/winstances.cpp
)

END()
