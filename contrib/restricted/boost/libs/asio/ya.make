LIBRARY()

WITHOUT_LICENSE_TEXTS() 
 
LICENSE(BSL-1.0)

VERSION(1.67)

OWNER( 
    antoshkka 
    g:cpp-committee 
    g:cpp-contrib 
) 

INCLUDE(${ARCADIA_ROOT}/contrib/restricted/boost/boost_common.inc)

PEERDIR(
    ${BOOST_ROOT}/libs/date_time
    ${BOOST_ROOT}/libs/system
    contrib/libs/openssl
)

END()
