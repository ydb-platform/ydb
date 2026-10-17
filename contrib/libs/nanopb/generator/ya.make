PY3_PROGRAM()

VERSION(Service-proxy-version)

LICENSE(
    MIT AND
    Zlib
)

WITHOUT_LICENSE_TEXTS()

SUBSCRIBER(
    g:cpp-contrib
    g:quasar-sys
)

NO_LINT()

PEERDIR(
    contrib/libs/nanopb/generator/proto
    contrib/python/protobuf
    contrib/python/setuptools
)

PY_SRCS(
    MAIN protoc-gen-nanopb.py
    nanopb_generator.py
    proto/__init__.py
    proto/_utils.py
)

END()
