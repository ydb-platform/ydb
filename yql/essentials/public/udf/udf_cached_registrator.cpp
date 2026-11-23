#include "udf_static_registry.h"

extern "C" const NYql::NUdf::TYqlUdfStaticLibrary* YQL_UDF_STATIC_ENTRY_POINT();

namespace {

const struct TCachedUdfRegistrator {
    TCachedUdfRegistrator() {
        NYql::NUdf::AddToStaticUdfRegistry(YQL_UDF_STATIC_ENTRY_POINT());
    }
} CachedUdfRegistrator;

} // namespace
