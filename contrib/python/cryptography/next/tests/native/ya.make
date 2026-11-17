PY3_LIBRARY()

VERSION(Service-proxy-version)

LICENSE(Apache-2.0 OR BSD-3-Clause)

PEERDIR(contrib/libs/librsvg)

SRCS(module.cpp)
PY_REGISTER(_rust_runtime_test)

END()
