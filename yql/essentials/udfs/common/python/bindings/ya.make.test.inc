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

PEERDIR(
    yql/essentials/minikql/computation/llvm16
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    yql/essentials/udfs/common/python/python_udf
)

YQL_LAST_ABI_VERSION()
