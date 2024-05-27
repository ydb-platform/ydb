IF (OS_LINUX)
    IF (NOT WITH_VALGRIND)
        UNITTEST_FOR(ydb/library/yql/udfs/common/python/bindings)

        SRCS(
            py_callable_ut.cpp
            py_cast_ut.cpp
            py_dict_ut.cpp
            py_list_ut.cpp
            py_decimal_ut.cpp
            py_number_ut.cpp
            py_optional_ut.cpp
            py_resource_ut.cpp
            py_stream_ut.cpp
            py_string_ut.cpp
            py_struct_ut.cpp
            py_tuple_ut.cpp
            py_tzdate_ut.cpp
            py_utils_ut.cpp
            py_variant_ut.cpp
            py_void_ut.cpp
        )

        USE_PYTHON3()

        PEERDIR(
            library/python/type_info
            ydb/library/yql/minikql/computation/llvm14
            ydb/library/yql/public/udf/service/exception_policy
            ydb/library/yql/sql/pg_dummy
        )

        YQL_LAST_ABI_VERSION()

        END()
    ENDIF()
ENDIF()
