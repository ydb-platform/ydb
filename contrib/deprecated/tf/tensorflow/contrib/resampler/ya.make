LIBRARY()

VERSION(1.10.1)

LICENSE(Apache-2.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

PEERDIR(
    contrib/deprecated/tf
)

NO_COMPILER_WARNINGS()

SRCS(
    GLOBAL kernels/resampler_ops.cc
    GLOBAL ops/resampler_ops.cc
)

END()
