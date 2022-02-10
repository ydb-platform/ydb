#include "yql_ydb_provider_impl.h"
#include <ydb/core/yq/libs/events/events.h>

#include <ydb/library/yql/providers/ydb/expr_nodes/yql_ydb_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/public/udf/udf_types.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/core/yq/libs/events/events.h>

namespace NYql {

namespace {

using namespace NNodes;

class TYdbIODiscoveryTransformer : public TGraphTransformerBase {
    using TDbId2Endpoint = THashMap<std::pair<TString, NYq::DatabaseType>, NYq::TEvents::TEvEndpointResponse::TEndpoint>;
public:
    TYdbIODiscoveryTransformer(TYdbState::TPtr state)
        : State_(std::move(state))
    {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;

        if (ctx.Step.IsDone(TExprStep::DiscoveryIO))
            return TStatus::Ok;

        if (!State_->DbResolver)
            return TStatus::Ok;

        THashMap<std::pair<TString, NYq::DatabaseType>, NYq::TEvents::TDatabaseAuth> ids;
        if (auto reads = FindNodes(input, [&](const TExprNode::TPtr& node) {
            const TExprBase nodeExpr(node);
            if (!nodeExpr.Maybe<TYdbRead>())
                return false;
            const TYdbRead read(node);
            if (read.DataSource().Category().Value() != YdbProviderName) {
                return false;
            }
            return true;
        }); !reads.empty()) {
            for (auto& node : reads) {
                const TYdbRead read(node);
                const auto& cluster = read.DataSource().Cluster().StringValue();
                const auto& dbId = State_->Configuration->Clusters[cluster].DatabaseId;
                const auto idKey = std::make_pair(dbId, NYq::DatabaseType::Ydb);
                const auto iter = State_->DatabaseIds.find(idKey);
                if (iter != State_->DatabaseIds.end()) {
                    ids[idKey] = iter->second;
                }
            }
        }
        if (ids.empty()) {
            return TStatus::Ok;
        }
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
        auto& clusters = State_->Configuration->Clusters;
        const auto& id2Clusters = State_->Configuration->DbId2Clusters;
        for (const auto& [dbIdWithType, info] : FullResolvedIds_) {
            const auto& dbId = dbIdWithType.first;
            const auto iter = id2Clusters.find(dbId);
            if (iter == id2Clusters.end()) {
                continue;
            }
            for (const auto& clusterName : iter->second) {
                auto& c = clusters[clusterName];
                c.Endpoint = info.Endpoint;
                c.Database = info.Database;
                c.Secure = info.Secure;
            }
        }
        return TStatus::Ok;
    }
private:
    const TYdbState::TPtr State_;

    NThreading::TFuture<void> AsyncFuture_;
    TDbId2Endpoint FullResolvedIds_;
    std::shared_ptr<TDbId2Endpoint> ResolvedIds_ = std::make_shared<TDbId2Endpoint>();
};
}

THolder<IGraphTransformer> CreateYdbIODiscoveryTransformer(TYdbState::TPtr state) {
    return THolder(new TYdbIODiscoveryTransformer(std::move(state)));
}

}
