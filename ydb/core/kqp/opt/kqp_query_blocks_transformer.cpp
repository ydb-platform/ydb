#include "kqp_opt_impl.h"

#include <yql/essentials/core/yql_graph_transformer.h>

#include <util/generic/yexception.h>

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
        if (TransformIsFinished) {
            return TStatus::Ok;
        }

        const TStatus status = DoTransformInternal(input, output, ctx);
        if (status == TStatus::Ok) {
            TransformIsFinished = true;
        }
        return status;
    }

    TStatus DoTransformInternal(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
        if (!TKqlQueryList::Match(input.Get())) {
            return TStatus::Error;
        }

        TKqlQueryList queryBlocks(input);
        TVector<TKqlQuery> transformedQueryBlocks;
        transformedQueryBlocks.reserve(queryBlocks.Size());

        // Already transformed previous time
        for (size_t transformedBlock = 0; transformedBlock < CurrentBlock; ++transformedBlock) {
            transformedQueryBlocks.emplace_back(queryBlocks.Item(transformedBlock));
        }

        // Current
        TStatus status = TStatus::Ok;
        for (; CurrentBlock < queryBlocks.Size(); ++CurrentBlock) {
            TExprNode::TPtr transformed = queryBlocks.Item(CurrentBlock).Ptr();
            status = AsyncTransformStep(*QueryBlockTransformer, transformed, ctx, false);
            transformedQueryBlocks.emplace_back(std::move(transformed));
            YQL_ENSURE(status.Level != TStatus::Repeat);
            if (status.Level == TStatus::Ok) {
                QueryBlockTransformer->Rewind();
                continue;
            }
            if (status.Level == TStatus::Error) {
                return status;
            }
            // Async
            break;
        }

        // Not yet transformed
        for (size_t nonTransformed = CurrentBlock + 1; nonTransformed < queryBlocks.Size(); ++nonTransformed) {
            transformedQueryBlocks.emplace_back(queryBlocks.Item(nonTransformed).Ptr());
        }

        output = Build<TKqlQueryList>(ctx, queryBlocks.Pos())
            .Add(transformedQueryBlocks)
            .Done().Ptr();

        return status;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) override {
        YQL_ENSURE(CurrentBlock < input.ChildrenSize());
        return QueryBlockTransformer->GetAsyncFuture(*input.Child(CurrentBlock));
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        return QueryBlockTransformer->ApplyAsyncChanges(input, output, ctx);
    }

private:
    TAutoPtr<IGraphTransformer> QueryBlockTransformer;
    size_t CurrentBlock = 0;
    bool TransformIsFinished = false;
};

} // namespace

TAutoPtr<IGraphTransformer> CreateKqpQueryBlocksTransformer(TAutoPtr<IGraphTransformer> queryBlockTransformer) {
    return new TKqpQueryBlocksTransformer(queryBlockTransformer);
}

} // namespace NKikimr::NKqp::NOpt
