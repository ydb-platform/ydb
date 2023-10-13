#include "yql_generic_provider_impl.h"

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>
#include <ydb/library/yql/utils/log/log.h>

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

                if (!State_->DatabaseResolver)
                    return TStatus::Ok;

                THashMap<std::pair<TString, NYql::EDatabaseType>, NYql::TDatabaseAuth> ids;
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

                        auto& cluster = State_->Configuration->ClusterNamesToClusterConfigs[clusterName];
                        auto databaseId = cluster.GetDatabaseId();
                        if (databaseId) {
                            YQL_CLOG(DEBUG, ProviderGeneric) << "found database id: " << databaseId;
                            const auto idKey = std::make_pair(databaseId, DatabaseTypeFromDataSourceKind(cluster.GetKind()));
                            const auto iter = State_->DatabaseAuth.find(idKey);
                            if (iter != State_->DatabaseAuth.end()) {
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
                const std::weak_ptr<NYql::TDatabaseResolverResponse> response = DbResolverResponse_;
                AsyncFuture_ = State_->DatabaseResolver->ResolveIds(ids).Apply([response](auto future) {
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
                auto databaseIdsToEndpointsResolved = std::move(DbResolverResponse_->DatabaseDescriptionMap);

                DbResolverResponse_ = std::make_shared<NYql::TDatabaseResolverResponse>();

                // Modify cluster configs with resolved ids
                return ModifyClusterConfigs(databaseIdsToEndpointsResolved, ctx);
            }

            void Rewind() final {
                AsyncFuture_ = {};
                DbResolverResponse_.reset(new NYql::TDatabaseResolverResponse);
            }

        private:
            TStatus ModifyClusterConfigs(const TDatabaseResolverResponse::TDatabaseDescriptionMap& databaseDescriptions, TExprContext& ctx) {
                const auto& databaseIdsToClusterNames = State_->Configuration->DatabaseIdsToClusterNames;
                auto& clusterNamesToClusterConfigs = State_->Configuration->ClusterNamesToClusterConfigs;

                for (const auto& [databaseIdWithType, databaseDescription] : databaseDescriptions) {
                    const auto& databaseId = databaseIdWithType.first;

                    Y_ENSURE(databaseDescription.Host, "Empty resolved database host");
                    Y_ENSURE(databaseDescription.Port, "Empty resolved database port");
                    YQL_CLOG(INFO, ProviderGeneric) << "resolved database id: " << databaseId
                                                    << ", host: " << databaseDescription.Host
                                                    << ", port: " << databaseDescription.Port;

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

                        auto endpointDst = clusterConfigIter->second.mutable_endpoint();
                        endpointDst->set_host(databaseDescription.Host);
                        endpointDst->set_port(databaseDescription.Port);
                    }
                }

                return TStatus::Ok;
            }

            const TGenericState::TPtr State_;

            NThreading::TFuture<void> AsyncFuture_;
            std::shared_ptr<NYql::TDatabaseResolverResponse> DbResolverResponse_ = std::make_shared<NYql::TDatabaseResolverResponse>();
        };
    }

    THolder<IGraphTransformer> CreateGenericIODiscoveryTransformer(TGenericState::TPtr state) {
        return THolder(new TGenericIODiscoveryTransformer(std::move(state)));
    }

}
