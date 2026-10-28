#include "udf_registrator.h"

extern "C" YQL_UDF_API const NYql::NUdf::TYqlUdfStaticLibrary* YQL_UDF_STATIC_ENTRY_POINT() {
    static constexpr NYql::NUdf::TYqlUdfStaticLibrary library{
        .StructSize = sizeof(NYql::NUdf::TYqlUdfStaticLibrary),
        .AbiVersion = NYql::NUdf::CurrentAbiVersion(),
        .Register = &Register,
        .SetBackTraceCallback = &SetBackTraceCallback,
    };
    return &library;
}
