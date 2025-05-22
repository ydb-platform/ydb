LIBRARY()

VERSION(1.1.1t)

LICENSE(
    Apache-2.0 AND
    BSD-2-Clause AND
    BSD-3-Clause AND
    BSD-Source-Code AND
    CC0-1.0 AND
    OpenSSL AND
    Public-Domain AND
    Snprintf
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

IF (OPENSOURCE_REPLACE_OPENSSL)

    OPENSOURCE_EXPORT_REPLACEMENT(
        CMAKE OpenSSL
        CMAKE_PACKAGE_COMPONENT Crypto
        CMAKE_TARGET OpenSSL::Crypto
        CONAN openssl/${OPENSOURCE_REPLACE_OPENSSL}
    )

ENDIF() # IF (OPENSOURCE_REPLACE_OPENSSL)

PEERDIR(
    contrib/libs/zlib
    library/cpp/sanitizer/include
)

ADDINCL(
    contrib/libs/openssl
    contrib/libs/openssl/crypto
    contrib/libs/openssl/crypto/ec/curve448
    contrib/libs/openssl/crypto/ec/curve448/arch_32
    contrib/libs/openssl/crypto/modes
    contrib/libs/openssl/include
)

CFLAGS(-DOPENSSL_BUILD=1)

IF (NOT EXPORT_CMAKE OR NOT OPENSOURCE_REPLACE_OPENSSL)
PEERDIR(contrib/libs/openssl)
ENDIF() # IF (NOT EXPORT_CMAKE OR NOT OPENSOURCE_REPLACE_OPENSSL)

END()
