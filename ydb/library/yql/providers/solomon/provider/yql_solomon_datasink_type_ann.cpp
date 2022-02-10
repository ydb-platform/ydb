#include "yql_solomon_provider_impl.h"

#include <ydb/library/yql/providers/solomon/expr_nodes/yql_solomon_expr_nodes.h>

namespace NYql {

using namespace NNodes;

class TSolomonDataSinkTypeAnnotationTransformer : public TVisitorTransformerBase {
public:
    TSolomonDataSinkTypeAnnotationTransformer(TSolomonState::TPtr state)
        : TVisitorTransformerBase(true)
        , State_(state)
    {
        using TSelf = TSolomonDataSinkTypeAnnotationTransformer;
        AddHandler({TSoWriteToShard::CallableName()}, Hndl(&TSelf::HandleWriteToShard));
        AddHandler({TSoShard::CallableName()}, Hndl(&TSelf::HandleSoShard));
        AddHandler({TCoCommit::CallableName()}, Hndl(&TSelf::HandleCommit));
    }

private:
    TStatus HandleWriteToShard(TExprBase input, TExprContext& ctx) {
        if (!EnsureArgsCount(input.Ref(), 4, ctx)) {
            return TStatus::Error;
        }
        TSoWriteToShard write = input.Cast<TSoWriteToShard>();
        if (!EnsureWorldType(write.World().Ref(), ctx)) {
            return TStatus::Error;
        }
        if (!EnsureSpecificDataSink(write.DataSink().Ref(), SolomonProviderName, ctx)) {
            return TStatus::Error;
        }
        if (!EnsureAtom(write.Shard().Ref(), ctx)) {
            return TStatus::Error;
        }
        input.Ptr()->SetTypeAnn(write.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    TStatus HandleSoShard(TExprBase input, TExprContext& ctx) {
        YQL_ENSURE(!State_->IsRtmrMode(), "SoShard can't be used in rtmr mode");

        if (!EnsureMinArgsCount(input.Ref(), 4, ctx) || !EnsureMaxArgsCount(input.Ref(), 5, ctx)) {
            return TStatus::Error;
        }

        const TSoShard shard = input.Cast<TSoShard>();

        if (!EnsureAtom(shard.SolomonCluster().Ref(), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(shard.Project().Ref(), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(shard.Cluster().Ref(), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(shard.Service().Ref(), ctx)) {
            return TStatus::Error;
        }

        if (shard.Token() && !EnsureCallable(shard.Token().Ref(), ctx)) {
            return TStatus::Error;
        }

        input.Ptr()->SetTypeAnn(ctx.MakeType<TUnitExprType>());
        return TStatus::Ok;
    }

    TStatus HandleCommit(TExprBase input, TExprContext& ctx) {
        Y_UNUSED(ctx);
        auto commit = input.Cast<TCoCommit>();
        input.Ptr()->SetTypeAnn(commit.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    TSolomonState::TPtr State_;
};

THolder<TVisitorTransformerBase> CreateSolomonDataSinkTypeAnnotationTransformer(TSolomonState::TPtr state) {
    return THolder(new TSolomonDataSinkTypeAnnotationTransformer(state)); 
}

} // namespace NYql
