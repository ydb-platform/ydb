#include "yql_solomon_provider_impl.h"

#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/solomon/expr_nodes/yql_solomon_expr_nodes.h>

/*
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <yql/library/statface_client/client.h>

#include <util/generic/hash_set.h>
*/

namespace NYql {

using namespace NNodes;

class TSolomonIODiscoveryTransformer : public TSyncTransformerBase {
public:
    TSolomonIODiscoveryTransformer(TSolomonState::TPtr state)
        : State_(state)
    {
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        output = input;

        if (ctx.Step.IsDone(ctx.Step.DiscoveryIO)) {
            return TStatus::Ok;
        }

        TVector<TIssue> issues;

        auto status = OptimizeExpr(input, output, [] (const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
            if (auto maybeWrite = TMaybeNode<TSoWrite>(node)) {
                if (!maybeWrite.DataSink()) {
                    return node;
                }
                auto write = maybeWrite.Cast();

                if (!EnsureArgsCount(write.Ref(), 5, ctx)) {
                    return {};
                }

                return Build<TSoWrite>(ctx, write.Pos())
                    .World(write.World())
                    .DataSink(write.DataSink())
                    .FreeArgs()
                        .Add<TCoAtom>()
                            .Value("")
                        .Build()
                        .Add(write.Arg(3))
                    .Build()
                    .Done().Ptr();

            } else if (TMaybeNode<TSoRead>(node).DataSource()) {
                return node;
            }
            return node;
        }, ctx, TOptimizeExprSettings {nullptr});

        if (issues) {
            for (const auto& issue: issues) {
                ctx.AddError(issue);
            }
            status = status.Combine(TStatus::Error);
        }

        return status;
    }

    void Rewind() final {
    }
private:
    TSolomonState::TPtr State_;
};

THolder<IGraphTransformer> CreateSolomonIODiscoveryTransformer(TSolomonState::TPtr state) {
    return THolder(new TSolomonIODiscoveryTransformer(state));
}

} // namespace NYql
