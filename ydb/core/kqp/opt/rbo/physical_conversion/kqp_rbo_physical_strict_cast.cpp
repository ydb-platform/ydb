#include "kqp_rbo_physical_strict_cast.h"

#include <yql/essentials/core/yql_expr_type_annotation.h>

#include <utility>

namespace NKikimr::NKqp::NPhysicalConvertionUtils {

using namespace NYql;

namespace {

bool IsScalarCastType(const TTypeAnnotationNode* type) {
    while (type && type->GetKind() == ETypeAnnotationKind::Optional) {
        type = type->Cast<TOptionalExprType>()->GetItemType();
    }
    return type && type->GetKind() == ETypeAnnotationKind::Data;
}

bool StrongCastMayFail(const TTypeAnnotationNode* source, const TTypeAnnotationNode* target) {
    return CastResult<true>(source, target) & NUdf::ECastOptions::MayFail;
}

TExprNode::TPtr ExpandDataCast(const TExprNode::TPtr& input, TExprContext& ctx) {
    const auto* from = input->Head().GetTypeAnn()->Cast<TDataExprType>();
    const auto* to = input->GetTypeAnn()->Cast<TDataExprType>();
    const auto fromFeatures = NUdf::GetDataTypeInfo(from->GetSlot()).Features;
    const auto toFeatures = NUdf::GetDataTypeInfo(to->GetSlot()).Features;

    if (((fromFeatures & NUdf::TzDateType) && (toFeatures & (NUdf::DateType | NUdf::TzDateType))) ||
        ((toFeatures & NUdf::TzDateType) && (fromFeatures & (NUdf::DateType | NUdf::TzDateType)))) {
        return ctx.Builder(input->Pos())
            .Callable("Apply")
                .Callable(0, "Udf")
                    .Atom(0, TString("DateTime2.Make") + to->GetName())
                .Seal()
                .Add(1, input->HeadPtr())
            .Seal().Build();
    }

    return ctx.RenameNode(*input, "SafeCast");
}

TExprNode::TPtr ExpandOptionalDataCast(const TExprNode::TPtr& input, TExprContext& ctx) {
    const auto* targetType = input->GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType();
    const auto options = CastResult<false>(input->Head().GetTypeAnn(), targetType);

    TExprNode::TPtr casted;
    if (options & NUdf::ECastOptions::MayFail) {
        casted = ctx.RenameNode(*input, "SafeCast");
    } else {
        casted = ctx.Builder(input->Pos())
            .Callable("Just")
                .Callable(0, "SafeCast")
                    .Add(0, input->HeadPtr())
                    .Add(1, ExpandType(input->Tail().Pos(), *targetType, ctx))
                .Seal()
            .Seal().Build();
    }

    if (options & NUdf::ECastOptions::MayLoseData) {
        casted = ctx.Builder(input->Pos())
            .Callable("Filter")
                .Add(0, std::move(casted))
                .Lambda(1)
                    .Param("casted")
                    .Callable("==")
                        .Add(0, input->HeadPtr())
                        .Arg(1, "casted")
                    .Seal()
                .Seal()
            .Seal().Build();
    }

    return casted;
}

TExprNode::TPtr ExpandOptionalCast(const TExprNode::TPtr& input, TExprContext& ctx) {
    const auto sourceType = input->Head().GetTypeAnn();
    const auto targetType = input->GetTypeAnn();
    const auto* sourceItemType = sourceType->Cast<TOptionalExprType>()->GetItemType();
    const auto* targetItemType = targetType->Cast<TOptionalExprType>()->GetItemType();
    const bool mayFail = StrongCastMayFail(sourceItemType, targetItemType);
    const auto sourceLevel = GetOptionalLevel(sourceItemType);
    const auto targetLevel = GetOptionalLevel(targetItemType);

    if (mayFail && targetLevel > 0U) {
        auto stub = ExpandType(input->Tail().Pos(), *targetType, ctx);
        auto type = ExpandType(input->Tail().Pos(), *targetItemType, ctx);

        if (sourceLevel == targetLevel) {
            return ctx.Builder(input->Pos())
                .Callable("FlatMap")
                    .Add(0, input->HeadPtr())
                    .Lambda(1)
                        .Param("item")
                        .Callable("If")
                            .Callable(0, "Or")
                                .Callable(0, "Exists")
                                    .Callable(0, "StrictCast")
                                        .Arg(0, "item")
                                        .Add(1, type)
                                    .Seal()
                                .Seal()
                                .Callable(1, "Not")
                                    .Callable(0, "Exists")
                                        .Arg(0, "item")
                                    .Seal()
                                .Seal()
                            .Seal()
                            .Callable(1, "Just")
                                .Callable(0, "StrictCast")
                                    .Arg(0, "item")
                                    .Add(1, std::move(type))
                                .Seal()
                            .Seal()
                            .Callable(2, "Nothing")
                                .Add(0, std::move(stub))
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal().Build();
        }

        if (sourceLevel < targetLevel) {
            auto casted = ctx.ChangeChild(*input, 1U, std::move(type));
            return ctx.Builder(input->Pos())
                .Callable("If")
                    .Callable(0, "Or")
                        .Callable(0, "Exists")
                            .Add(0, casted)
                        .Seal()
                        .Callable(1, "Not")
                            .Callable(0, "Exists")
                                .Add(0, input->HeadPtr())
                            .Seal()
                        .Seal()
                    .Seal()
                    .Callable(1, "Just")
                        .Add(0, std::move(casted))
                    .Seal()
                    .Callable(2, "Nothing")
                        .Add(0, std::move(stub))
                    .Seal()
                .Seal().Build();
        }
    }

    const bool flat = mayFail || sourceLevel > targetLevel;
    auto type = ExpandType(input->Tail().Pos(), flat ? *targetType : *targetItemType, ctx);
    if (!mayFail && sourceLevel < targetLevel) {
        return ctx.Builder(input->Pos())
            .Callable("Just")
                .Callable(0, "StrictCast")
                    .Add(0, input->HeadPtr())
                    .Add(1, std::move(type))
                .Seal()
            .Seal().Build();
    }

    return ctx.Builder(input->Pos())
        .Callable(flat ? "FlatMap" : "Map")
            .Add(0, input->HeadPtr())
            .Lambda(1)
                .Param("item")
                .Callable("StrictCast")
                    .Arg(0, "item")
                    .Add(1, std::move(type))
                .Seal()
            .Seal()
        .Seal().Build();
}

} // anonymous namespace

TExprNode::TPtr ExpandScalarStrictCast(const TExprNode::TPtr& input, TExprContext& ctx) {
    YQL_ENSURE(input->IsCallable("StrictCast"), "Expected StrictCast, got " << input->Content());
    YQL_ENSURE(
        input->Head().GetTypeAnn() && input->GetTypeAnn(),
        "StrictCast must be type-annotated before expansion");
    YQL_ENSURE(
        IsScalarCastType(input->Head().GetTypeAnn()) && IsScalarCastType(input->GetTypeAnn()),
        "KQP physical conversion supports StrictCast only over Data and its Optional wrappers; got "
            << *input->Head().GetTypeAnn() << " to " << *input->GetTypeAnn());
    YQL_ENSURE(
        !(CastResult<true>(input->Head().GetTypeAnn(), input->GetTypeAnn()) & NUdf::ECastOptions::Impossible),
        "KQP physical conversion cannot expand an impossible StrictCast from "
            << *input->Head().GetTypeAnn() << " to " << *input->GetTypeAnn());

    const auto sourceKind = input->Head().GetTypeAnn()->GetKind();
    const auto targetKind = input->GetTypeAnn()->GetKind();
    if (sourceKind == targetKind) {
        switch (sourceKind) {
            case ETypeAnnotationKind::Data:
                return ExpandDataCast(input, ctx);
            case ETypeAnnotationKind::Optional:
                return ExpandOptionalCast(input, ctx);
            default:
                break;
        }
    } else if (targetKind == ETypeAnnotationKind::Optional) {
        const auto* targetItemType = input->GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType();
        auto type = ExpandType(input->Tail().Pos(), *targetItemType, ctx);
        if (StrongCastMayFail(input->Head().GetTypeAnn(), targetItemType)) {
            if (sourceKind == ETypeAnnotationKind::Data && targetItemType->GetKind() == ETypeAnnotationKind::Data) {
                return ExpandOptionalDataCast(input, ctx);
            }

            return ctx.Builder(input->Pos())
                .Callable("Map")
                    .Callable(0, "StrictCast")
                        .Add(0, input->HeadPtr())
                        .Add(1, std::move(type))
                    .Seal()
                    .Lambda(1)
                        .Param("item")
                        .Callable("Just")
                            .Arg(0, "item")
                        .Seal()
                    .Seal()
                .Seal().Build();
        }

        return ctx.Builder(input->Pos())
            .Callable("Just")
                .Callable(0, "StrictCast")
                    .Add(0, input->HeadPtr())
                    .Add(1, std::move(type))
                .Seal()
            .Seal().Build();
    }

    YQL_ENSURE(
        false,
        "KQP physical conversion cannot expand StrictCast from "
            << *input->Head().GetTypeAnn() << " to " << *input->GetTypeAnn());
    return input;
}

} // namespace NKikimr::NKqp::NPhysicalConvertionUtils
