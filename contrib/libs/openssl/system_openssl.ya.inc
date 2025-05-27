LIBRARY()

WITHOUT_LICENSE_TEXTS()

SUBSCRIBER(g:cpp-contrib)
PROVIDES(openssl)

ORIGINAL_SOURCE(https://github.com/openssl/openssl)
VERSION(system-version)
LICENSE(OpenSSL)

NO_RUNTIME()

IF (NOT SYSTEM_OPENSSL_INCLUDE)
    FATAL("You should provide path to OpenSSL include dir via -DSYSTEM_OPENSSL_INCLUDE=/abs/path/to/openssl/includes")
ENDIF()

IF (NOT SYSTEM_OPENSSL_LIB)
    FATAL("You should provide path to static OpenSSL library via -DSYSTEM_OPENSSL_LIB=/abs/path/to/openssl/lib/dir")
ENDIF()

EXTRALIBS_STATIC($SYSTEM_OPENSSL_LIB/libcrypto.a)
EXTRALIBS_STATIC($SYSTEM_OPENSSL_LIB/libssl.a)
CFLAGS(GLOBAL -I$SYSTEM_OPENSSL_INCLUDE)

END()
