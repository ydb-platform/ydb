PY23_NATIVE_LIBRARY()

WITHOUT_LICENSE_TEXTS()

LICENSE(BSL-1.0)

OWNER(
    antoshkka
    g:cpp-committee
    g:cpp-contrib
)

PEERDIR(
    contrib/restricted/boost/libs/python 
)

NO_COMPILER_WARNINGS()

SRCS(
    module.cpp
)

PY_REGISTER(
    arcadia_boost_python_test
)

END()
