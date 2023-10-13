#include "yql_ydb_provider_impl.h"

#include <ydb/library/yql/providers/ydb/expr_nodes/yql_ydb_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/public/udf/udf_types.h>
#include <ydb/library/yql/ast/yql_expr.h>

namespace NYql {

namespace {

using namespace NNodes;

class TYdbIODiscoveryTransformer : public TGraphTransformerBase {
using TDbId2Endpoint = THashMap<std::pair<TString, NYql::EDatabaseType>, NYql::TDatabaseResolverResponse::TDatabaseDescription>;
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

        THashMap<std::pair<TString, NYql::EDatabaseType>, NYql::TDatabaseAuth> ids;
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
                const auto idKey = std::make_pair(dbId, NYql::EDatabaseType::Ydb);
                const auto iter = State_->DatabaseIds.find(idKey);
                if (iter != State_->DatabaseIds.end()) {
                    ids[idKey] = iter->second;
                }
            }
        }
        if (ids.empty()) {
            return TStatus::Ok;
        }
        const std::weak_ptr<NYql::TDatabaseResolverResponse> response = DbResolverResponse_;
        AsyncFuture_ = State_->DbResolver->ResolveIds(ids).Apply([response](auto future)
        {
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

    void Rewind() final {
        AsyncFuture_ = {};
        FullResolvedIds_.clear();
        DbResolverResponse_ = std::make_shared<NYql::TDatabaseResolverResponse>();
    }
private:
    const TYdbState::TPtr State_;

    NThreading::TFuture<void> AsyncFuture_;
    TDbId2Endpoint FullResolvedIds_;
    std::shared_ptr<NYql::TDatabaseResolverResponse> DbResolverResponse_ = std::make_shared<NYql::TDatabaseResolverResponse>();
};
}

THolder<IGraphTransformer> CreateYdbIODiscoveryTransformer(TYdbState::TPtr state) {
    return THolder(new TYdbIODiscoveryTransformer(std::move(state)));
}

}
