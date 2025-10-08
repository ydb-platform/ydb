#include "udf_types.h"

namespace NYql {
namespace NUdf {

ITypeVisitor::ITypeVisitor(ui16 compatibilityVersion)
    : TBase(compatibilityVersion)
{
}

ITypeInfoHelper::ITypeInfoHelper()
{
}

} // namespace NUdf
} // namespace NYql
