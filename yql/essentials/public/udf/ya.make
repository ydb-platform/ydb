YQL_LIBRARY()

SRCS(
    udf_allocator.cpp
    udf_allocator.h
    udf_counter.cpp
    udf_counter.h
    udf_data_type.cpp
    udf_data_type.h
    udf_helpers.cpp
    udf_helpers.h
    udf_log.h
    udf_log.cpp
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
    udf_version.h
)

PEERDIR(
    library/cpp/deprecated/enum_codegen
    yql/essentials/public/udf/abi_version_check
    library/cpp/resource
    yql/essentials/public/decimal
    yql/essentials/public/types
    library/cpp/deprecated/atomic
)

# The two builds of the SDK are different modules to the check behind PROVIDES: a static udf
# builds a current variant for current binaries, and its closure may keep the stable SDK below a
# plain LIBRARY. Mixing the builds within a binary is caught by the feature version check.
IF (MODULE_TAG == "YQL_ABI_CURRENT")
    PROVIDES(YqlUdfSdkCurrent)
ELSE()
    PROVIDES(YqlUdfSdk)
ENDIF()

END()

RECURSE(
    abi_version_check
    sanitizer_utils
    arrow
    service
    support
)

RECURSE_FOR_TESTS(
    udf_debug_checks_ut
    ut
)
