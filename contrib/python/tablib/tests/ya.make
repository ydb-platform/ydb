PY3TEST()

NO_LINT()

ALL_PYTEST_SRCS()

PEERDIR(
    contrib/python/pandas
    contrib/python/tablib
    contrib/python/MarkupPy
    contrib/python/PyYAML
    contrib/python/odfpy
    contrib/python/openpyxl
    contrib/python/xlrd
    contrib/python/xlwt
    contrib/python/tabulate
)

DATA(
    arcadia/contrib/python/tablib/tests/files
)

END()
