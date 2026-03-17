PY3TEST()

PEERDIR(
    contrib/python/qrcode
    contrib/python/Pillow
    contrib/python/pypng
)

NO_LINT()

SRCDIR(contrib/python/qrcode/py3)

PY_SRCS(
    TOP_LEVEL
    qrcode/tests/consts.py
)

TEST_SRCS(
    qrcode/tests/__init__.py
    qrcode/tests/test_example.py
    qrcode/tests/test_qrcode_pil.py
    qrcode/tests/test_qrcode.py
    qrcode/tests/test_qrcode_pypng.py
    qrcode/tests/test_qrcode_svg.py
    qrcode/tests/test_release.py
    qrcode/tests/test_script.py
    qrcode/tests/test_util.py
)

END()