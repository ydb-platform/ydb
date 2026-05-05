#pragma once

#include "udf_string_ref.h"
#include "udf_types.h"
#include "udf_type_inspection.h"

namespace NYql::NUdf {

class TTypePrinter {
public:
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 25)
    TTypePrinter(const ITypeInfoHelper2& typeHelper, const TType* type)
#else
    TTypePrinter(const ITypeInfoHelper1& typeHelper, const TType* type)
#endif
        : TypeHelper_(typeHelper)
        , Type_(type)
    {
    }

    void Out(IOutputStream& o) const;

private:
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 25)
    const ITypeInfoHelper2& TypeHelper_;
#else
    const ITypeInfoHelper1& TypeHelper_;
#endif
    const TType* Type_;
};

} // namespace NYql::NUdf
