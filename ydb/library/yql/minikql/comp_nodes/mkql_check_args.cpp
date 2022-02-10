#include "mkql_check_args.h"
#include <ydb/library/yql/minikql/mkql_node_builder.h>

namespace NKikimr {
namespace NMiniKQL {

TBinaryFunctionArgsDesc CheckBinaryFunctionArgs(
        TType* left, TType* right,
        bool allowOptionalInput, bool requiresBooleanArgs)
{
    TBinaryFunctionArgsDesc desc;
    const auto& leftType = UnpackOptional(left, desc.IsLeftOptional);
    MKQL_ENSURE(leftType->IsData(), "Expected data");

    const auto& rightType = UnpackOptional(right, desc.IsRightOptional);
    MKQL_ENSURE(rightType->IsData(), "Expected data");

    if (!allowOptionalInput) {
        MKQL_ENSURE(!desc.IsLeftOptional && !desc.IsRightOptional,
                    "Optional are not expected here");
    }

    const auto& leftDataType = static_cast<const TDataType&>(*leftType);
    const auto& rightDataType = static_cast<const TDataType&>(*rightType);
    MKQL_ENSURE(leftDataType.GetSchemeType() == rightDataType.GetSchemeType(),
                "Mismatch data types");

    desc.SchemeType = leftDataType.GetSchemeType();
    if (requiresBooleanArgs) {
        MKQL_ENSURE(desc.SchemeType == NUdf::TDataType<bool>::Id, "Expected bool");
    }
    return desc;
}

}
}
