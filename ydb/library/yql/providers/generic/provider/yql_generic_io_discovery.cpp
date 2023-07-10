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
                        const auto clusterName = read.DataSource().Cluster().StringValue();
                        YQL_CLOG(DEBUG, ProviderGeneric) << "found cluster name: " << clusterName;

                        auto databaseId = State_->Configuration->ClusterNamesToClusterConfigs[clusterName].GetDatabaseId();
                        if (databaseId) {
                            YQL_CLOG(DEBUG, ProviderGeneric) << "found database id: " << databaseId;
                            const auto idKey = std::make_pair(databaseId, NYql::DatabaseType::Generic);
                            const auto iter = State_->DatabaseIds.find(idKey);
                            if (iter != State_->DatabaseIds.end()) {
                                YQL_CLOG(DEBUG, ProviderGeneric) << "resolve database id: " << databaseId;
                                ids[idKey] = iter->second;
                            }
                        }
                    }
                }
                YQL_CLOG(DEBUG, ProviderGeneric) << "total database ids to resolve: " << ids.size();

                if (ids.empty()) {
                    return TStatus::Ok;
                }

                // FIXME: overengineered code - instead of using weak_ptr, directly copy shared_ptr in callback in this way:
                // Apply([response = DbResolverResponse_](...))
                const std::weak_ptr<NYql::TDbResolverResponse> response = DbResolverResponse_;
                AsyncFuture_ = State_->DbResolver->ResolveIds(ids).Apply([response](auto future) {
                    if (const auto res = response.lock()) {
                        *res = std::move(future.ExtractValue());
                    }
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

                // Copy resolver results and reallocate pointer
                auto databaseIdsToEndpointsResolved = std::move(DbResolverResponse_->DatabaseId2Endpoint);

                DbResolverResponse_ = std::make_shared<NYql::TDbResolverResponse>();

                // Modify cluster configs with resolved ids
                return ModifyClusterConfigs(databaseIdsToEndpointsResolved, ctx);
            }

            void Rewind() final {
                AsyncFuture_ = {};
                DbResolverResponse_.reset(new NYql::TDbResolverResponse);
            }

        private:
            TStatus ModifyClusterConfigs(const TDbResolverResponse::TDatabaseEndpointsMap& databaseIdsToEndpoints, TExprContext& ctx) {
                const auto& databaseIdsToClusterNames = State_->Configuration->DatabaseIdsToClusterNames;
                auto& clusterNamesToClusterConfigs = State_->Configuration->ClusterNamesToClusterConfigs;

                for (const auto& [databaseIdWithType, endpointSrc] : databaseIdsToEndpoints) {
                    const auto& databaseId = databaseIdWithType.first;

                    YQL_CLOG(DEBUG, ProviderGeneric) << "resolved database id: " << databaseId
                                                     << ",  endpoint: " << endpointSrc.Endpoint;

                    auto clusterNamesIter = databaseIdsToClusterNames.find(databaseId);

                    if (clusterNamesIter == databaseIdsToClusterNames.cend()) {
                        TIssues issues;
                        issues.AddIssue(TStringBuilder() << "no cluster names for database id " << databaseId);
                        ctx.IssueManager.AddIssues(issues);
                        return TStatus::Error;
                    }

                    for (const auto& clusterName : clusterNamesIter->second) {
                        auto clusterConfigIter = clusterNamesToClusterConfigs.find(clusterName);

                        if (clusterConfigIter == clusterNamesToClusterConfigs.end()) {
                            TIssues issues;
                            issues.AddIssue(TStringBuilder() << "no cluster names for database id " << databaseIdWithType.first << " and cluster name " << clusterName);
                            ctx.IssueManager.AddIssues(issues);
                            return TStatus::Error;
                        }

                        auto hostPort = endpointSrc.ParseHostPort();
                        auto endpointDst = clusterConfigIter->second.mutable_endpoint();
                        endpointDst->set_host(std::get<0>(hostPort));
                        endpointDst->set_port(std::get<1>(hostPort));
                    }
                }

                return TStatus::Ok;
            }

            const TGenericState::TPtr State_;

            NThreading::TFuture<void> AsyncFuture_;
            std::shared_ptr<NYql::TDbResolverResponse> DbResolverResponse_ = std::make_shared<NYql::TDbResolverResponse>();
        };
    }

    THolder<IGraphTransformer> CreateGenericIODiscoveryTransformer(TGenericState::TPtr state) {
        return THolder(new TGenericIODiscoveryTransformer(std::move(state)));
    }

}
