PY3_LIBRARY()

LICENSE(GPL-2.0-or-later)

VERSION(3.1.0-2888-g9d86712)

NO_LINT()
PY_SRCS(
    TOP_LEVEL
    qemu/__init__.py
    qemu/qmp.py
    qemu/qtest.py
)
END()
