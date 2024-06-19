#include "yql_solomon_provider_impl.h"

namespace NYql {

using namespace NNodes;

class TSolomonLoadTableMetadataTransformer : public TGraphTransformerBase {
public:
    TSolomonLoadTableMetadataTransformer(TSolomonState::TPtr state)
        : State_(state)
    {
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        Y_UNUSED(input);
        Y_UNUSED(output);

        if (ctx.Step.IsDone(TExprStep::LoadTablesMetadata)) {
            return TStatus::Ok;
        }

        return TStatus::Ok;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) final {
        Y_UNUSED(input);
        return AsyncFuture_;
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext&) final {
        output = input;

        return TStatus::Ok;
    }

    void Rewind() final {
    }

private:
    TSolomonState::TPtr State_;
    NThreading::TFuture<void> AsyncFuture_;
};

THolder<IGraphTransformer> CreateSolomonLoadTableMetadataTransformer(TSolomonState::TPtr state) {
    return THolder(new TSolomonLoadTableMetadataTransformer(state));
}

} // namespace NYql
