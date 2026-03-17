PY3_LIBRARY()

LICENSE(BSD-3-Clause)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

# 306a54e6
VERSION(2018-10-25)

PEERDIR(
    contrib/libs/eigen
    contrib/libs/opengv/src
    contrib/libs/pybind11
)

ADDINCL(
    contrib/libs/eigen
    contrib/libs/opengv/include
)

NO_COMPILER_WARNINGS()

SRCS(
    pyopengv.cpp
)

PY_REGISTER(pyopengv)

END()
