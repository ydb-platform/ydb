SUBSCRIBER(g:ymake)

PY23_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    _metric_resolvers.py=lib._metric_resolvers
)

PEERDIR(
    build/plugins/lib
)

END()
