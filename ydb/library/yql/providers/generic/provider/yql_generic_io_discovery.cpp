#include "yql_generic_provider_impl.h"

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>

namespace NYql {

    namespace {

        using namespace NNodes;

        class TGenericIODiscoveryTransformer: public TGraphTransformerBase {
            using TDbId2Endpoint = THashMap<std::pair<TString, NYql::DatabaseType>, NYql::TDbResolverResponse::TEndpoint>;

        public:
            TGenericIODiscoveryTransformer(TGenericState::TPtr state)
                : State_(std::move(state))
            {
            }

            TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
                output = input;

                if (ctx.Step.IsDone(TExprStep::DiscoveryIO))
                    return TStatus::Ok;

                if (!State_->DbResolver)
                    return TStatus::Ok;

                THashMap<std::pair<TString, NYql::DatabaseType>, NYql::TDatabaseAuth> ids;
                if (auto reads = FindNodes(input,
                                           [&](const TExprNode::TPtr& node) {
                                               const TExprBase nodeExpr(node);
                                               if (!nodeExpr.Maybe<TGenRead>())
                                                   return false;

                                               auto read = nodeExpr.Maybe<TGenRead>().Cast();
                                               if (read.DataSource().Category().Value() != GenericProviderName) {
                                                   return false;
                                               }
                                               return true;
                                           });
                    !reads.empty()) {
                    for (auto& node : reads) {
                        const TGenRead read(node);
                        const auto cluster = read.DataSource().Cluster().StringValue();
                        YQL_CLOG(DEBUG, ProviderGeneric) << "Found cluster: " << cluster;
                        auto databaseID = State_->Configuration->ClusterConfigs[cluster].GetDatabaseID();
                        YQL_CLOG(DEBUG, ProviderGeneric) << "Found cloudID: " << databaseID;
                        const auto idKey = std::make_pair(databaseID, NYql::DatabaseType::Generic);
                        const auto iter = State_->DatabaseIds.find(idKey);
                        if (iter != State_->DatabaseIds.end()) {
                            YQL_CLOG(DEBUG, ProviderGeneric) << "Resolve CloudID: " << databaseID;
                            ids[idKey] = iter->second;
                        }
                    }
                }
                YQL_CLOG(DEBUG, ProviderGeneric) << "Ids to resolve: " << ids.size();

                if (ids.empty()) {
                    return TStatus::Ok;
                }

                const std::weak_ptr<NYql::TDbResolverResponse> response = DbResolverResponse_;
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
                FullResolvedIds_.insert(DbResolverResponse_->DatabaseId2Endpoint.begin(),
                                        DbResolverResponse_->DatabaseId2Endpoint.end());
                DbResolverResponse_ = std::make_shared<NYql::TDbResolverResponse>();
                YQL_CLOG(DEBUG, ProviderGeneric) << "ResolvedIds: " << FullResolvedIds_.size();
                const auto& id2Clusters = State_->Configuration->DbId2Clusters;
                for (const auto& [dbIdWithType, info] : FullResolvedIds_) {
                    const auto& dbId = dbIdWithType.first;
                    const auto iter = id2Clusters.find(dbId);
                    if (iter == id2Clusters.end()) {
                        continue;
                    }
                }
                return TStatus::Ok;
            }

            void Rewind() final {
                AsyncFuture_ = {};
                FullResolvedIds_.clear();
                DbResolverResponse_.reset(new NYql::TDbResolverResponse);
            }

        private:
            const TGenericState::TPtr State_;

            NThreading::TFuture<void> AsyncFuture_;
            TDbId2Endpoint FullResolvedIds_;
            std::shared_ptr<NYql::TDbResolverResponse> DbResolverResponse_;
        };
    }

    THolder<IGraphTransformer> CreateGenericIODiscoveryTransformer(TGenericState::TPtr state) {
        return THolder(new TGenericIODiscoveryTransformer(std::move(state)));
    }

}
