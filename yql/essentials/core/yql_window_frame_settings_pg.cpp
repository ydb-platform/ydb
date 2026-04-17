#include "yql_window_frame_settings_pg.h"

#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/parser/pg_wrapper/interface/compare.h>
#include <yql/essentials/parser/pg_wrapper/interface/sign.h>

namespace NYql {

std::expected<TInRangeCasts, TString> LookupInRangeCasts(
    const TTypeAnnotationNode* columnType,
    const TTypeAnnotationNode* offsetType,
    TPositionHandle pos,
    TExprContext& ctx)
{
    YQL_ENSURE(columnType && columnType->GetKind() == ETypeAnnotationKind::Pg);
    YQL_ENSURE(offsetType && offsetType->GetKind() == ETypeAnnotationKind::Pg);

    const auto columnTypeId = columnType->Cast<TPgExprType>()->GetId();
    const auto offsetTypeId = offsetType->Cast<TPgExprType>()->GetId();
    const auto& columnTypeName = NPg::LookupType(columnTypeId).Name;
    const auto& offsetTypeName = NPg::LookupType(offsetTypeId).Name;
    const auto boolTypeId = NPg::LookupType("bool").TypeId;

    TVector<ui32> argTypes = {columnTypeId, columnTypeId, offsetTypeId, boolTypeId, boolTypeId};

    try {
        const auto resolution = ResolvePgCall("in_range", argTypes, pos, ctx);

        const auto* procRes = resolution.AsProc();
        if (!procRes) {
            return std::unexpected(TStringBuilder()
                                   << "No suitable in_range function for column type '" << columnTypeName
                                   << "' and offset type '" << offsetTypeName << "'");
        }
        YQL_ENSURE(procRes->InputTransforms.size() >= 3);
        if (procRes->Proc->ResultType != NPg::LookupType("bool").TypeId) {
            return std::unexpected(TStringBuilder()
                                   << "Result type mismatch for in_range function: expected bool, got " << NPg::LookupType(procRes->Proc->ResultType).Name);
        }

        return TInRangeCasts{.ColumnCast = procRes->InputTransforms[0], .OffsetCast = procRes->InputTransforms[2], .ProcId = procRes->Proc->ProcId};
    } catch (const yexception& e) {
        return std::unexpected(TStringBuilder()
                               << "Failed to resolve in_range for column type '" << columnTypeName
                               << "' and offset type '" << offsetTypeName << "': " << e.what());
    }
}

std::expected<std::strong_ordering, TString> PgSign(const TExprNode::TPtr& value) {
    if (!value->IsCallable("PgConst")) {
        return std::unexpected("Expected literal of pg type");
    }

    const auto* typeAnn = value->GetTypeAnn();
    YQL_ENSURE(typeAnn && typeAnn->GetKind() == ETypeAnnotationKind::Pg);

    const auto typeId = typeAnn->Cast<TPgExprType>()->GetId();
    const auto& valueContent = value->Child(0)->Content();

    return PgSign(valueContent, typeId);
}

std::expected<bool, TString> PgCompareWithCasts(
    const TExprNode::TPtr& lhs,
    const TExprNode::TPtr& rhs,
    NKikimr::NMiniKQL::EPgCompareType cmpType)
{
    if (!lhs->IsCallable("PgConst")) {
        return std::unexpected("Expected literal of pg type");
    }
    if (!rhs->IsCallable("PgConst")) {
        return std::unexpected("Expected literal of pg type");
    }

    const auto* lhsTypeAnn = lhs->GetTypeAnn();
    YQL_ENSURE(lhsTypeAnn && lhsTypeAnn->GetKind() == ETypeAnnotationKind::Pg);

    const auto* rhsTypeAnn = rhs->GetTypeAnn();
    YQL_ENSURE(rhsTypeAnn && rhsTypeAnn->GetKind() == ETypeAnnotationKind::Pg);

    const auto lhsTypeId = lhsTypeAnn->Cast<TPgExprType>()->GetId();
    const auto& lhsContent = lhs->Child(0)->Content();

    const auto rhsTypeId = rhsTypeAnn->Cast<TPgExprType>()->GetId();
    const auto& rhsContent = rhs->Child(0)->Content();

    return NKikimr::NMiniKQL::PgCompareWithCasts(lhsContent, lhsTypeId, rhsContent, rhsTypeId, cmpType);
}

} // namespace NYql
