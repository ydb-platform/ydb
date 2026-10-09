PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    fqrun.py
    kqprun.py
)

PEERDIR(
    yql/essentials/tests/common/test_framework
)

END()
