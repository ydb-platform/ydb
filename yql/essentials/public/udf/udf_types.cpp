#include "udf_types.h"

namespace NYql::NUdf {

ITypeVisitor::ITypeVisitor(ui16 compatibilityVersion)
    : TBase(compatibilityVersion)
{
}

ITypeInfoHelper::ITypeInfoHelper() = default;

} // namespace NYql::NUdf
