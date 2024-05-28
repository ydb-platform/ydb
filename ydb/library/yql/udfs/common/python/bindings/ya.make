PY23_NATIVE_LIBRARY()

YQL_ABI_VERSION(2 27 0)

SRCS(
    py_callable.cpp
    py_cast.cpp
    py_decimal.cpp
    py_errors.cpp
    py_dict.cpp
    py_list.cpp
    py_lazy_mkql_dict.cpp
    py_lazy_mkql_list.cpp
    py_iterator.cpp
    py_resource.cpp
    py_stream.cpp
    py_struct.cpp
    py_tuple.cpp
    py_utils.cpp
    py_variant.cpp
    py_void.cpp
    py_yql_module.cpp
)

IF (USE_SYSTEM_PYTHON AND _SYSTEM_PYTHON27)
    # we should be able to run on python 2.7.X versions
    # with X ranging from 3 to (at least) 15
    #
    # for now bindings already use some functionality from 2.7.15,
    # which doesn't exist earlier versions
    # (according symbols won't be loaded from system python)
    #
    # so we provide backported implementation for this scenario to work as intended
    SRCS(
        py27_backports.c
    )
ENDIF()

RESOURCE(
    typing.py typing.py
)

PEERDIR(
    ydb/library/yql/public/udf
    ydb/library/yql/utils
)

NO_COMPILER_WARNINGS()

END()

RECURSE_FOR_TESTS(
    ut3
)
