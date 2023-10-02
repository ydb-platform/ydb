PY3_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(3.3.2)

IF (PYTHON2)
    PEERDIR(
        contrib/deprecated/python/enum34
        contrib/deprecated/python/ipaddress
    )
ENDIF()

PEERDIR(
    contrib/libs/openssl
    contrib/python/cffi
    # contrib/python/idna
    # contrib/python/asn1crypto
    contrib/python/setuptools
    contrib/python/six
)

NO_COMPILER_WARNINGS()
NO_LINT()

RESOURCE_FILES(
    PREFIX contrib/python/cryptography/py3/
    .dist-info/METADATA
    .dist-info/top_level.txt
)

SRCS(
    build/temp.linux-x86_64-2.7/_openssl.c
    build/temp.linux-x86_64-2.7/_padding.c
)

PY_REGISTER(
    cryptography.hazmat.bindings._openssl
    cryptography.hazmat.bindings._padding
)

PY_SRCS(
    TOP_LEVEL
    cryptography/__about__.py
    cryptography/__init__.py
    cryptography/exceptions.py
    cryptography/fernet.py
    cryptography/hazmat/__init__.py
    cryptography/hazmat/_der.py
    cryptography/hazmat/_oid.py
    cryptography/hazmat/backends/__init__.py
    cryptography/hazmat/backends/interfaces.py
    cryptography/hazmat/backends/openssl/__init__.py
    cryptography/hazmat/backends/openssl/aead.py
    cryptography/hazmat/backends/openssl/backend.py
    cryptography/hazmat/backends/openssl/ciphers.py
    cryptography/hazmat/backends/openssl/cmac.py
    cryptography/hazmat/backends/openssl/decode_asn1.py
    cryptography/hazmat/backends/openssl/dh.py
    cryptography/hazmat/backends/openssl/dsa.py
    cryptography/hazmat/backends/openssl/ec.py
    cryptography/hazmat/backends/openssl/ed25519.py
    cryptography/hazmat/backends/openssl/ed448.py
    cryptography/hazmat/backends/openssl/encode_asn1.py
    cryptography/hazmat/backends/openssl/hashes.py
    cryptography/hazmat/backends/openssl/hmac.py
    cryptography/hazmat/backends/openssl/ocsp.py
    cryptography/hazmat/backends/openssl/poly1305.py
    cryptography/hazmat/backends/openssl/rsa.py
    cryptography/hazmat/backends/openssl/utils.py
    cryptography/hazmat/backends/openssl/x25519.py
    cryptography/hazmat/backends/openssl/x448.py
    cryptography/hazmat/backends/openssl/x509.py
    cryptography/hazmat/bindings/__init__.py
    cryptography/hazmat/bindings/openssl/__init__.py
    cryptography/hazmat/bindings/openssl/_conditional.py
    cryptography/hazmat/bindings/openssl/binding.py
    cryptography/hazmat/primitives/__init__.py
    cryptography/hazmat/primitives/asymmetric/__init__.py
    cryptography/hazmat/primitives/asymmetric/dh.py
    cryptography/hazmat/primitives/asymmetric/dsa.py
    cryptography/hazmat/primitives/asymmetric/ec.py
    cryptography/hazmat/primitives/asymmetric/ed25519.py
    cryptography/hazmat/primitives/asymmetric/ed448.py
    cryptography/hazmat/primitives/asymmetric/padding.py
    cryptography/hazmat/primitives/asymmetric/rsa.py
    cryptography/hazmat/primitives/asymmetric/utils.py
    cryptography/hazmat/primitives/asymmetric/x25519.py
    cryptography/hazmat/primitives/asymmetric/x448.py
    cryptography/hazmat/primitives/ciphers/__init__.py
    cryptography/hazmat/primitives/ciphers/aead.py
    cryptography/hazmat/primitives/ciphers/algorithms.py
    cryptography/hazmat/primitives/ciphers/base.py
    cryptography/hazmat/primitives/ciphers/modes.py
    cryptography/hazmat/primitives/cmac.py
    cryptography/hazmat/primitives/constant_time.py
    cryptography/hazmat/primitives/hashes.py
    cryptography/hazmat/primitives/hmac.py
    cryptography/hazmat/primitives/kdf/__init__.py
    cryptography/hazmat/primitives/kdf/concatkdf.py
    cryptography/hazmat/primitives/kdf/hkdf.py
    cryptography/hazmat/primitives/kdf/kbkdf.py
    cryptography/hazmat/primitives/kdf/pbkdf2.py
    cryptography/hazmat/primitives/kdf/scrypt.py
    cryptography/hazmat/primitives/kdf/x963kdf.py
    cryptography/hazmat/primitives/keywrap.py
    cryptography/hazmat/primitives/padding.py
    cryptography/hazmat/primitives/poly1305.py
    cryptography/hazmat/primitives/serialization/__init__.py
    cryptography/hazmat/primitives/serialization/base.py
    cryptography/hazmat/primitives/serialization/pkcs12.py
    cryptography/hazmat/primitives/serialization/pkcs7.py
    cryptography/hazmat/primitives/serialization/ssh.py
    cryptography/hazmat/primitives/twofactor/__init__.py
    cryptography/hazmat/primitives/twofactor/hotp.py
    cryptography/hazmat/primitives/twofactor/totp.py
    cryptography/hazmat/primitives/twofactor/utils.py
    cryptography/utils.py
    cryptography/x509/__init__.py
    cryptography/x509/base.py
    cryptography/x509/certificate_transparency.py
    cryptography/x509/extensions.py
    cryptography/x509/general_name.py
    cryptography/x509/name.py
    cryptography/x509/ocsp.py
    cryptography/x509/oid.py
)

END()
