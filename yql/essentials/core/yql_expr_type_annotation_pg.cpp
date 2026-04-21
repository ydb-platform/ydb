#include "yql_expr_type_annotation_pg.h"

#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>

#include <util/generic/overloaded.h>
#include <util/string/cast.h>

namespace NYql {

namespace {

TVector<TExprNode::TPtr> ApplyInputTransforms(
    const TVector<TMaybe<TNodeTransform>>& transforms,
    const TVector<TExprNode::TPtr>& inputArgNodes,
    TExprContext& ctx)
{
    YQL_ENSURE(transforms.size() == inputArgNodes.size());
    TVector<TExprNode::TPtr> result;
    result.reserve(inputArgNodes.size());
    for (size_t i = 0; i < inputArgNodes.size(); ++i) {
        if (transforms[i]) {
            result.push_back((*transforms[i])(inputArgNodes[i], ctx));
        } else {
            result.push_back(inputArgNodes[i]);
        }
    }
    return result;
}
} // namespace


bool IsCastRequired(ui32 fromTypeId, ui32 toTypeId) {
    if (toTypeId == fromTypeId) {
        return false;
    }
    if (toTypeId == NPg::AnyOid || toTypeId == NPg::AnyArrayOid || toTypeId == NPg::AnyNonArrayOid) {
        return false;
    }
    return true;
}

TExprNodePtr WrapWithPgCast(TExprNodePtr node, ui32 targetTypeId, TExprContext& ctx) {
    return ctx.Builder(node->Pos())
        .Callable("PgCast")
            .Add(0, std::move(node))
            .Callable(1, "PgType")
                .Atom(0, NPg::LookupType(targetTypeId).Name)
                .Seal()
        .Seal()
        .Build();
}

TPgCallResolutionResult ResolvePgCall(
    const TString& name,
    const TVector<ui32>& argTypes,
    TPositionHandle pos,
    TExprContext& ctx)
{
    const auto procOrType = NPg::LookupProcWithCasts(name, argTypes);

    return std::visit(TOverloaded{
        [&](const NPg::TProcDesc* procPtr) -> TPgCallResolutionResult {
            const auto& proc = *procPtr;
            TPgCallResolutionResult::TProc result;
            result.Proc = &proc;

            const auto& fargTypes = proc.ArgTypes;
            for (size_t i = 0; i < argTypes.size(); ++i) {
                auto targetType = (i >= fargTypes.size()) ? proc.VariadicType : fargTypes[i];
                if (IsCastRequired(argTypes[i], targetType)) {
                    auto arg = ctx.NewArgument(pos, "from");
                    auto body = WrapWithPgCast(arg, targetType, ctx);
                    auto lambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {arg}), std::move(body));
                    result.InputTransforms.push_back(TNodeTransform(std::move(lambda)));
                } else {
                    result.InputTransforms.push_back(Nothing());
                }
            }

            if (argTypes.size() < fargTypes.size()) {
                YQL_ENSURE(fargTypes.size() - argTypes.size() <= proc.DefaultArgs.size());
                for (size_t i = argTypes.size(); i < fargTypes.size(); ++i) {
                    const auto& value = proc.DefaultArgs[i + proc.DefaultArgs.size() - fargTypes.size()];
                    TExprNode::TPtr defNode;
                    if (!value) {
                        defNode = ctx.NewCallable(pos, "Null", {});
                    } else {
                        // clang-format off
                        defNode = ctx.Builder(pos)
                            .Callable("PgConst")
                                .Atom(0, *value)
                                .Callable(1, "PgType")
                                    .Atom(0, NPg::LookupType(fargTypes[i]).Name)
                                    .Seal()
                                .Seal()
                            .Build();
                        // clang-format on
                    }
                    result.DefaultArgs.push_back(std::move(defNode));
                }
            }

            return TPgCallResolutionResult{std::move(result)};
        },
        [&](const NPg::TTypeDesc* typePtr) -> TPgCallResolutionResult {
            return TPgCallResolutionResult{typePtr};
        }
    }, procOrType);
}

TVector<TExprNode::TPtr> TPgCallResolutionResult::TProc::BuildArgs(
    const TVector<TExprNode::TPtr>& inputArgNodes,
    TExprContext& ctx) const {
    auto result = ApplyInputTransforms(InputTransforms, inputArgNodes, ctx);
    for (const auto& defArg : DefaultArgs) {
        result.push_back(defArg);
    }
    return result;
}

TMaybe<TPgCallResolutionResult> ResolvePgCall(
    const TString& name,
    const TVector<TExprNode::TPtr>& inputArgNodes,
    TPositionHandle pos,
    TExprContext& ctx)
{
    TVector<ui32> argTypes;
    argTypes.reserve(inputArgNodes.size());
    for (const auto& node : inputArgNodes) {
        ui32 argType;
        bool convertToPg;
        bool isUniversal;
        if (!ExtractPgType(node->GetTypeAnn(), argType, convertToPg, node->Pos(), ctx, isUniversal)) {
            return Nothing();
        }
        YQL_ENSURE(!convertToPg, "PG call " << name << ": convertToPg must be false");
        YQL_ENSURE(!isUniversal, "PG call " << name << ": isUniversal must be false");
        argTypes.push_back(argType);
    }
    return ResolvePgCall(name, argTypes, pos, ctx);
}

} // namespace NYql
