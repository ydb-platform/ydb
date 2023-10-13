#include "yql_clickhouse_provider_impl.h"
#include <ydb/library/yql/providers/clickhouse/expr_nodes/yql_clickhouse_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/ast/yql_expr.h>


namespace NYql {

namespace {

using namespace NNodes;

class TClickHouseIODiscoveryTransformer : public TGraphTransformerBase {

using TDbId2Endpoint = THashMap<std::pair<TString, NYql::EDatabaseType>, NYql::TDatabaseResolverResponse::TDatabaseDescription>;

public:
    TClickHouseIODiscoveryTransformer(TClickHouseState::TPtr state)
        : State_(std::move(state))
    {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;

        if (ctx.Step.IsDone(TExprStep::DiscoveryIO))
            return TStatus::Ok;

        if (!State_->DbResolver)
            return TStatus::Ok;

        THashMap<std::pair<TString, NYql::EDatabaseType>, NYql::TDatabaseAuth> ids;
        if (auto reads = FindNodes(input, [&](const TExprNode::TPtr& node) {
            const TExprBase nodeExpr(node);
            if (!nodeExpr.Maybe<TClRead>())
                return false;

            auto read = nodeExpr.Maybe<TClRead>().Cast();
            if (read.DataSource().Category().Value() != ClickHouseProviderName) {
                return false;
            }
            return true;
        }); !reads.empty()) {
            for (auto& node : reads) {
                const TClRead read(node);
                const auto cluster = read.DataSource().Cluster().StringValue();
                YQL_CLOG(DEBUG, ProviderClickHouse) << "Found cluster: " << cluster;
                auto dbId = State_->Configuration->Endpoints[cluster].first;
                dbId = dbId.substr(0, dbId.find(':'));
                YQL_CLOG(DEBUG, ProviderClickHouse) << "Found dbId: " << dbId;
                const auto idKey = std::make_pair(dbId, NYql::EDatabaseType::ClickHouse);
                const auto iter = State_->DatabaseIds.find(idKey);
                if (iter != State_->DatabaseIds.end()) {
                    YQL_CLOG(DEBUG, ProviderClickHouse) << "Resolve CH id: " << dbId;
                    ids[idKey] = iter->second;
                }
            }
        }
        YQL_CLOG(DEBUG, ProviderClickHouse) << "Ids to resolve: " << ids.size();

        if (ids.empty()) {
            return TStatus::Ok;
        }

        const std::weak_ptr<NYql::TDatabaseResolverResponse> response = DbResolverResponse_;
        AsyncFuture_ = State_->DbResolver->ResolveIds(ids).Apply([response](auto future) {
            if (const auto res = response.lock())
                *res = std::move(future.ExtractValue());
        });
        return TStatus::Async;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode&) final {
        return AsyncFuture_;
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;
        AsyncFuture_.GetValue();
        if (!DbResolverResponse_->Success) {
            ctx.IssueManager.AddIssues(DbResolverResponse_->Issues);
            return TStatus::Error;
        }
        FullResolvedIds_.insert(DbResolverResponse_->DatabaseDescriptionMap.begin(), DbResolverResponse_->DatabaseDescriptionMap.end());
        DbResolverResponse_ = std::make_shared<NYql::TDatabaseResolverResponse>();
        YQL_CLOG(DEBUG, ProviderClickHouse) << "ResolvedIds: " << FullResolvedIds_.size();
        auto& endpoints = State_->Configuration->Endpoints;
        const auto& id2Clusters = State_->Configuration->DbId2Clusters;
        for (const auto& [dbIdWithType, info] : FullResolvedIds_) {
            const auto& dbId = dbIdWithType.first;
            const auto iter = id2Clusters.find(dbId);
            if (iter == id2Clusters.end()) {
                continue;
            }
            for (const auto& clusterName : iter->second) {
                YQL_CLOG(DEBUG, ProviderClickHouse) << "Resolved endpoint: " << info.Endpoint << " for id: "<< dbId;
                auto& endpoint = endpoints[clusterName].first;
                auto& secure = endpoints[clusterName].second;
                if (const auto it = endpoint.find(':'); it != TString::npos) {
                    secure = info.Secure;
                    const auto port = endpoint.substr(it);
                    endpoint = info.Endpoint;
                    if (info.Endpoint.find(':') == TString::npos)
                        endpoint += port;
                }
            }
        }
        return TStatus::Ok;
    }

    void Rewind() final {
        AsyncFuture_ = {};
        FullResolvedIds_.clear();
        DbResolverResponse_ = std::make_shared<NYql::TDatabaseResolverResponse>();
    }
private:
    const TClickHouseState::TPtr State_;

    NThreading::TFuture<void> AsyncFuture_;
    TDbId2Endpoint FullResolvedIds_;
    std::shared_ptr<NYql::TDatabaseResolverResponse> DbResolverResponse_ = std::make_shared<NYql::TDatabaseResolverResponse>();
};
}

THolder<IGraphTransformer> CreateClickHouseIODiscoveryTransformer(TClickHouseState::TPtr state) {
    return THolder(new TClickHouseIODiscoveryTransformer(std::move(state)));
}

}
