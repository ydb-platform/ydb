#include "yql_pq_provider_impl.h"
#include "yql_pq_helpers.h"

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/utils/log/log.h> 
#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>

namespace NYql {

namespace {

using namespace NNodes;

class TPqIODiscoveryTransformer : public TGraphTransformerBase {

using TDbId2Endpoint = THashMap<std::pair<TString, NYq::DatabaseType>, NYq::TEvents::TEvEndpointResponse::TEndpoint>;

public:
    explicit TPqIODiscoveryTransformer(TPqState::TPtr state)
        : State_(state)
    {
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        Y_UNUSED(ctx);
        output = input;
        if (ctx.Step.IsDone(TExprStep::DiscoveryIO))
            return TStatus::Ok;

        if (!State_->DbResolver)
            return TStatus::Ok;

        THashMap<std::pair<TString, NYq::DatabaseType>, NYq::TEvents::TDatabaseAuth> ids;
        FindYdsDbIdsForResolving(State_, input, ids);

        if (ids.empty())
            return TStatus::Ok;

        AsyncFuture_ = State_->DbResolver->ResolveIds({ids, State_->DbResolver->GetTraceId()}).Apply([resolvedIds_ = ResolvedIds_](const auto& future) {
            *resolvedIds_ = future.GetValue();
        });
        return TStatus::Async;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode&) final {
        return AsyncFuture_;
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext&) final {
        output = input;
        AsyncFuture_.GetValue();
        FullResolvedIds_.insert(ResolvedIds_->begin(), ResolvedIds_->end());
        ResolvedIds_->clear();
        FillSettingsWithResolvedYdsIds(State_, FullResolvedIds_);
        return TStatus::Ok;
    }

private:
    const TPqState::TPtr State_;
    NThreading::TFuture<void> AsyncFuture_;
    TDbId2Endpoint FullResolvedIds_;
    std::shared_ptr<TDbId2Endpoint> ResolvedIds_ = std::make_shared<TDbId2Endpoint>();
};

}

THolder<IGraphTransformer> CreatePqIODiscoveryTransformer(TPqState::TPtr state) {
    return THolder(new TPqIODiscoveryTransformer(state));
}

}
