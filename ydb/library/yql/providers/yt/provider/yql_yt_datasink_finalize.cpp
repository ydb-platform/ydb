#include "yql_yt_provider_impl.h"

#include <ydb/library/yql/core/yql_graph_transformer.h>

namespace NYql {

using namespace NNodes;

class TYtDataSinkFinalizingTransformer: public TAsyncCallbackTransformer<TYtDataSinkFinalizingTransformer> {
public:
    TYtDataSinkFinalizingTransformer(TYtState::TPtr state)
        : State_(state)
    {
    }

    std::pair<TStatus, TAsyncTransformCallbackFuture> CallbackTransform(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        Y_UNUSED(ctx);
        output = input;
        auto future = State_->Gateway->Finalize(
            IYtGateway::TFinalizeOptions(State_->SessionId)
                .Config(State_->Configuration->Snapshot())
                .Abort(State_->Types->HiddenMode == EHiddenMode::Force)
                .DetachSnapshotTxs(State_->PassiveExecution)
        );
        return WrapFuture(future, [](const IYtGateway::TFinalizeResult& res, const TExprNode::TPtr& input, TExprContext& ctx) {
            Y_UNUSED(res);
            return ctx.NewWorld(input->Pos());
        });
    }

private:
    TYtState::TPtr State_;
};

THolder<IGraphTransformer> CreateYtDataSinkFinalizingTransformer(TYtState::TPtr state) {
    return THolder(new TYtDataSinkFinalizingTransformer(state));
}

} // NYql
