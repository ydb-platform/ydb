LIBRARY()

LICENSE(BSL-1.0) 
 
LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

OWNER(
    antoshkka
    g:cpp-committee
    g:cpp-contrib
)

INCLUDE(${ARCADIA_ROOT}/contrib/restricted/boost/boost_common.inc)

SRCS(
    src/cmdline.cpp
    src/config_file.cpp
    src/convert.cpp
    src/options_description.cpp
    src/parsers.cpp
    src/positional_options.cpp
    src/split.cpp
    src/utf8_codecvt_facet.cpp
    src/value_semantic.cpp
    src/variables_map.cpp
    src/winmain.cpp
)

END()
