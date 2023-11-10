PY3TEST()

WITHOUT_LICENSE_TEXTS()

NO_LINT()

SIZE(MEDIUM)

TEST_SRCS(
    tjunittest.py
    bittests.py
)

DEPENDS(
    contrib/libs/libjpeg-turbo/cjpeg
    contrib/libs/libjpeg-turbo/djpeg
    contrib/libs/libjpeg-turbo/jpegtran
    contrib/libs/libjpeg-turbo/tjunittest
)

DATA(arcadia/contrib/libs/libjpeg-turbo/testimages)

END()
