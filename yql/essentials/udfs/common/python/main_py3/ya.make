LIBRARY()

USE_PYTHON3()

ADDINCL(
    yql/essentials/udfs/common/python/main_py3/include
)

SRCS(GLOBAL main.cpp)

BUILDWITH_CYTHON_C(main.pyx --embed=RunPythonImpl)

END()
