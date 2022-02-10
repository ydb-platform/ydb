LIBRARY()

LICENSE(BSL-1.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

OWNER(
    antoshkka
    g:cpp-committee
    g:cpp-contrib
)

INCLUDE(${ARCADIA_ROOT}/contrib/restricted/boost/boost_common.inc)

IF (DYNAMIC_BOOST) 
    CFLAGS(
        -DBOOST_FILESYSTEM_DYN_LINK=1
    )
ELSE() 
    CFLAGS(
        -DBOOST_FILESYSTEM_STATIC_LINK=1
    )
ENDIF() 

PEERDIR(
    ${BOOST_ROOT}/libs/system
)

SRCS(
    src/codecvt_error_category.cpp
    src/operations.cpp
    src/path.cpp
    src/path_traits.cpp
    src/portability.cpp
    src/unique_path.cpp
    src/utf8_codecvt_facet.cpp
    src/windows_file_codecvt.cpp
)

END()
