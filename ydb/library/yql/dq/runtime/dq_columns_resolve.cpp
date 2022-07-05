#include "dq_columns_resolve.h"

#include <ydb/library/yql/utils/yql_panic.h>

namespace NYql::NDq {

using namespace NKikimr::NMiniKQL;

TMaybe<TColumnInfo> FindColumnInfo(const NKikimr::NMiniKQL::TType* type, TStringBuf columnName) {
    YQL_ENSURE(type->GetKind() == TType::EKind::Struct);
    const auto& structType = static_cast<const TStructType&>(*type);

    auto columnIndex = structType.FindMemberIndex(columnName);
    if (!columnIndex) {
        return {};
    }

    auto memberType = structType.GetMemberType(*columnIndex);

    if (memberType->GetKind() == TType::EKind::Optional) {
        memberType = static_cast<TOptionalType&>(*memberType).GetItemType();
    }

    return TColumnInfo{TString(columnName), *columnIndex, memberType};
}

TColumnInfo GetColumnInfo(const TType* type, TStringBuf columnName) {
    auto columnInfo = FindColumnInfo(type, columnName);
    YQL_ENSURE(columnInfo);

    return *columnInfo;
}

}
