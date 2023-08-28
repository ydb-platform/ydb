LIBRARY()

SRCS(
    udf_allocator.cpp
    udf_allocator.h
    udf_counter.cpp
    udf_counter.h
    udf_data_type.cpp
    udf_data_type.h
    udf_helpers.cpp
    udf_helpers.h
    udf_pg_type_description.h
    udf_ptr.h
    udf_registrator.cpp
    udf_registrator.h
    udf_static_registry.cpp
    udf_static_registry.h
    udf_string.cpp
    udf_string.h
    udf_type_builder.cpp
    udf_type_builder.h
    udf_type_inspection.cpp
    udf_type_inspection.h
    udf_type_ops.h
    udf_type_printer.cpp
    udf_type_printer.h
    udf_type_size_check.h
    udf_types.cpp
    udf_types.h
    udf_ut_helpers.h
    udf_validate.cpp
    udf_validate.h
    udf_value.cpp
    udf_value.h
    udf_value_builder.cpp
    udf_value_builder.h
    udf_value_inl.h
    udf_version.cpp
    udf_version.h
)

PEERDIR(
    library/cpp/deprecated/enum_codegen
    library/cpp/resource
    ydb/library/yql/public/decimal
    ydb/library/yql/public/types
    library/cpp/deprecated/atomic
)

YQL_LAST_ABI_VERSION()

PROVIDES(YqlUdfSdk)

END()

RECURSE(
    arrow
    service
    support
    tz
)

RECURSE_FOR_TESTS(
    ut
)
