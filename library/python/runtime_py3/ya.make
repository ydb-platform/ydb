PY3_LIBRARY()

STYLE_PYTHON()

PEERDIR(
    library/cpp/resource
)

IF (USE_PYTHON3_PREV)
    PEERDIR(
        contrib/tools/python3_prev
        contrib/tools/python3_prev/lib2/py
    )
ELSE()
    PEERDIR(
        contrib/tools/python3
        contrib/tools/python3/lib2/py
    )
ENDIF()

NO_PYTHON_INCLUDES()

ENABLE(PYBUILD_NO_PYC)

SRCS(
    __res.cpp
    sitecustomize.cpp
    GLOBAL runtime_reg_py3.cpp
)

PY_SRCS(
    entry_points.py
)

IF (EXTERNAL_PY_FILES)
    PEERDIR(
        library/python/runtime_py3/enable_external_py_files
    )
ENDIF()

RUN_PROGRAM(
    library/python/runtime_py3/stage0pycc
        mod=${MODDIR}/__res.py __res.py __res.pyc
        mod=${MODDIR}/sitecustomize.py sitecustomize.py sitecustomize.pyc
    IN __res.py sitecustomize.py
    OUT_NOAUTO __res.pyc sitecustomize.pyc
    ENV PYTHONHASHSEED=0
)
ARCHIVE(NAME __res.pyc.inc DONTCOMPRESS __res.pyc)
ARCHIVE(NAME sitecustomize.pyc.inc DONTCOMPRESS sitecustomize.pyc)

END()

RECURSE(
    enable_external_py_files
)

RECURSE_FOR_TESTS(
    test
)
