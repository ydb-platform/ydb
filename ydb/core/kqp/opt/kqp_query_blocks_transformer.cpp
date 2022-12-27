#include "kqp_opt_impl.h"

#include <ydb/library/yql/core/yql_graph_transformer.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

using TStatus = IGraphTransformer::TStatus;

namespace {

class TKqpQueryBlocksTransformer : public TGraphTransformerBase {
public:
    TKqpQueryBlocksTransformer(TAutoPtr<IGraphTransformer> queryBlockTransformer)
        : QueryBlockTransformer(queryBlockTransformer) {
    }

    void Rewind() override {
        QueryBlockTransformer->Rewind();
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        if (!TKqlQueryList::Match(input.Get())) {
            return TStatus::Error;
        }

        TKqlQueryList queryBlocks(input);
        TVector<TKqlQuery> transformedQueryBlocks;
        transformedQueryBlocks.reserve(queryBlocks.Size());
        for (auto queryBlock : queryBlocks) {
            auto transformed = queryBlock.Ptr();
            auto status = InstantTransform(*QueryBlockTransformer, transformed, ctx);
            transformedQueryBlocks.emplace_back(std::move(transformed));

            if (status.Level != IGraphTransformer::TStatus::Ok) {
                return status;
            }

            QueryBlockTransformer->Rewind();
        }

        output = Build<TKqlQueryList>(ctx, queryBlocks.Pos())
            .Add(transformedQueryBlocks)
            .Done().Ptr();

        return TStatus::Ok;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) override {
        Y_UNUSED(input);
        return NThreading::MakeFuture();
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        Y_UNUSED(ctx);
        output = input;
        return TStatus::Ok;
    }

private:
    TAutoPtr<IGraphTransformer> QueryBlockTransformer;
};

} // namespace

TAutoPtr<IGraphTransformer> CreateKqpQueryBlocksTransformer(TAutoPtr<IGraphTransformer> queryBlockTransformer) {
    return new TKqpQueryBlocksTransformer(queryBlockTransformer);
}

} // namespace NKikimr::NKqp::NOpt


