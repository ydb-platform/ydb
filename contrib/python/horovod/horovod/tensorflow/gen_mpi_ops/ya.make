PY3_LIBRARY()

WITHOUT_LICENSE_TEXTS()

VERSION(Service-proxy-version)

LICENSE(Apache-2.0)

PEERDIR(
    contrib/python/horovod/horovod/tensorflow
)

NO_LINT()

RUN_PROGRAM(
    contrib/python/horovod/horovod/tensorflow/gen_mpi_ops/gen ${__COMMA__} 0
    STDOUT_NOAUTO __init__.py
)

PY_SRCS(
    NAMESPACE horovod.tensorflow.gen_mpi_ops
    __init__.py
)

END()

RECURSE(
    gen
)
