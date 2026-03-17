PY3TEST()

NO_LINT()

PEERDIR(
    contrib/python/opentelemetry-instrumentation-aio-pika
    contrib/python/opentelemetry-semantic-conventions
    contrib/python/aio-pika
)

ALL_PYTEST_SRCS(RECURSIVE)

SIZE(MEDIUM)

END()
