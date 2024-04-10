PY23_LIBRARY()

PY_SRCS(
    encoder.pyx
    metric.pyx
    metric_registry.pyx
)

PEERDIR(
    library/cpp/monlib/metrics
    library/cpp/monlib/encode/json
    library/cpp/monlib/encode/spack
    library/cpp/monlib/encode/unistat
)

END()

RECURSE_FOR_TESTS(
    ut/py2
    ut/py3
)
