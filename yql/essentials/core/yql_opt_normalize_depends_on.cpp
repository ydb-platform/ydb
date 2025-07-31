#include "yql_opt_normalize_depends_on.h"

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

#include <util/string/hex.h>

namespace NYql {

namespace {

class TNormalizeDependsOnTransformer : public TSyncTransformerBase {
public:
    TNormalizeDependsOnTransformer(const TTypeAnnotationContext& types)
        : Types_(types)
    {}

    IGraphTransformer::TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        if (!Types_.NormalizeDependsOn) {
            return IGraphTransformer::TStatus::Ok;
        }

        if (ctx.Step.IsDone(TExprStep::NormalizeDependsOn)) {
            return IGraphTransformer::TStatus::Ok;
        }

        YQL_PROFILE_SCOPE(DEBUG, "TNormalizeDependsOnTransformer::DoTransform");

        std::vector<TExprNode::TPtr> toNormalize;
        VisitExpr(input, [&](const TExprNode::TPtr& node) {
            // Only InnerDependsOn can be normalized
            if (node->IsCallable("InnerDependsOn") && !IsNormalizedDependsOn(*node)) {
                toNormalize.push_back(node);
                return false;
            }

            return true;
        });

        if (toNormalize.empty()) {
            ctx.Step.Done(TExprStep::NormalizeDependsOn);
            return IGraphTransformer::TStatus::Ok;
        }

        TNodeOnNodeOwnedMap replaces;
        for (const auto& dependsOn : toNormalize) {
            TExprNode::TListType normalizedArgs;

            auto hash = HexEncode(MakeCacheKey(dependsOn->Head()));
            normalizedArgs.push_back(ctx.NewCallable(dependsOn->Head().Pos(), "String", { ctx.NewAtom(dependsOn->Head().Pos(), hash) }));

            TNodeSet innerLambdasArgs, outerLambdasArgs;
            VisitExpr(dependsOn, [&](const TExprNode::TPtr& node) {
                if (node->GetDependencyScope() && node->IsComplete()) {
                    return false;
                }

                if (node->IsLambda()) {
                    for (const auto& arg: node->Head().Children()) {
                        innerLambdasArgs.insert(arg.Get());
                    }
                } else if (node->IsArgument()) {
                    if (!innerLambdasArgs.contains(node.Get()) && outerLambdasArgs.insert(node.Get()).second) {
                        normalizedArgs.push_back(node);
                    }
                }

                return true;
            }, [&](const TExprNode::TPtr& node) {
                if (node->IsLambda()) {
                    for (const auto& arg: node->Head().Children()) {
                        innerLambdasArgs.erase(arg.Get());
                    }
                }

                return true;
            });

            if (normalizedArgs.size() == 1) {
                replaces[dependsOn.Get()] = ctx.ChangeChild(*dependsOn, 0, std::move(normalizedArgs[0]));
            } else {
                replaces[dependsOn.Get()] = ctx.ChangeChild(*dependsOn, 0, ctx.NewList(dependsOn->Pos(), std::move(normalizedArgs)));
            }
        }

        YQL_CLOG(INFO, Core) << "NormalizeDependsOn";
        output = ctx.ReplaceNodes(std::move(input), replaces);
        ctx.Step.Done(TExprStep::NormalizeDependsOn);
        return TStatus(IGraphTransformer::TStatus::Repeat, true);
    }

    void Rewind() override {
    }

private:
    const TTypeAnnotationContext& Types_;
};

}

THolder<IGraphTransformer> CreateNormalizeDependsOnTransformer(const TTypeAnnotationContext& types) {
    return THolder<IGraphTransformer>(new TNormalizeDependsOnTransformer(types));
}

} // namespace NYql
