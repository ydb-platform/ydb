#include "phy_opt/yql_yt_phy_opt.h"

#include <ydb/library/yql/core/yql_graph_transformer.h>


#include <util/generic/ptr.h>


namespace NYql {

namespace {

using namespace NNodes;


class TAsyncSyncCompositeTransformer : public TGraphTransformerBase {
public:
    TAsyncSyncCompositeTransformer(THolder<IGraphTransformer>&& async, THolder<IGraphTransformer>&& sync)
        : Async(std::move(async))
        , Sync(std::move(sync))
    {
    }
private:
    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        auto status = Async->Transform(input, output, ctx);
        if (status.Level != TStatus::Ok) {
            return status;
        }
        return InstantTransform(*Sync, output, ctx, true);
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) override {
        return Async->GetAsyncFuture(input);
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        return Async->ApplyAsyncChanges(input, output, ctx);
    }

    void Rewind() final {
        Async->Rewind();
        Sync->Rewind();
    }

    const THolder<IGraphTransformer> Async;
    const THolder<IGraphTransformer> Sync;

};

} // namespce

THolder<IGraphTransformer> CreateYtPhysicalOptProposalTransformer(TYtState::TPtr state) {
    return MakeHolder<TAsyncSyncCompositeTransformer>(CreateYtLoadColumnarStatsTransformer(state), MakeHolder<TYtPhysicalOptProposalTransformer>(state));
}

} // namespace NYql
