#include "dq_columns_resolve.h"

#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NDq {

using namespace NKikimr::NMiniKQL;

TMaybe<TColumnInfo> FindColumnInfo(const NKikimr::NMiniKQL::TType* type, TStringBuf columnName) {
    TType* memberType = nullptr;
    ui32 idx;
    TMaybe<bool> isScalar;
    if (type->GetKind() == TType::EKind::Multi) {
        const auto& multiType = static_cast<const TMultiType&>(*type);
        YQL_ENSURE(TryFromString(columnName, idx), "Expecting number as column name");
        YQL_ENSURE(idx < multiType.GetElementsCount(), "Invalid column index");
        memberType = multiType.GetElementType(idx);
        if (memberType->IsBlock()) {
            auto blockType = static_cast<const TBlockType*>(memberType);
            isScalar = blockType->GetShape() == TBlockType::EShape::Scalar;
            memberType = blockType->GetItemType();
        }
    } else {
        YQL_ENSURE(type->GetKind() == TType::EKind::Struct);
        const auto& structType = static_cast<const TStructType&>(*type);
        auto columnIndex = structType.FindMemberIndex(columnName);
        if (!columnIndex) {
             return {};
        }
        memberType = structType.GetMemberType(*columnIndex);
        idx = *columnIndex;
    }

    if (memberType->GetKind() == TType::EKind::Optional) {
        memberType = static_cast<TOptionalType&>(*memberType).GetItemType();
    }

    return TColumnInfo{TString(columnName), idx, memberType, isScalar};
}

TColumnInfo GetColumnInfo(const TType* type, TStringBuf columnName) {
    auto columnInfo = FindColumnInfo(type, columnName);
    YQL_ENSURE(columnInfo);

    return *columnInfo;
}

}
