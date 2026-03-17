PY2TEST()

PEERDIR(
    contrib/python/qrcode
    contrib/python/mock
    contrib/python/Pillow
)

SRCDIR(contrib/python/qrcode/py2/qrcode/tests)

PY_SRCS(
    NAMESPACE qrcode.tests
    svg.py
)

TEST_SRCS(
    test_example.py
    test_qrcode.py
    test_release.py
    test_script.py
)

END()