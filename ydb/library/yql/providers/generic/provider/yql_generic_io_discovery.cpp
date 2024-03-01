#include "yql_generic_provider_impl.h"
#include "yql_generic_utils.h"

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

                auto reads = FindNodes(input,
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
                if (reads.empty()) {
                    return TStatus::Ok;
                }

                // Collect clusters that are in need of resolving
                IDatabaseAsyncResolver::TDatabaseAuthMap unresolvedClusters;
                for (auto& node : reads) {
                    const TGenRead read(node);
                    const auto clusterName = read.DataSource().Cluster().StringValue();
                    YQL_CLOG(DEBUG, ProviderGeneric) << "discovered cluster name: " << clusterName;

                    auto& cluster = State_->Configuration->ClusterNamesToClusterConfigs[clusterName];
                    auto databaseId = cluster.GetDatabaseId();
                    if (databaseId) {
                        YQL_CLOG(DEBUG, ProviderGeneric) << "discovered database id: " << databaseId;
                        const auto idKey = std::make_pair(databaseId, DatabaseTypeFromDataSourceKind(cluster.GetKind()));
                        const auto iter = State_->DatabaseAuth.find(idKey);
                        if (iter != State_->DatabaseAuth.end()) {
                            YQL_CLOG(DEBUG, ProviderGeneric) << "requesting to resolve"
                                                             << ": clusterName=" << clusterName
                                                             << ", databaseId=" << databaseId;

                            unresolvedClusters[idKey] = iter->second;
                        }
                    }
                }

                if (unresolvedClusters.empty()) {
                    return TStatus::Ok;
                }

                YQL_CLOG(DEBUG, ProviderGeneric) << "total database ids to be resolved: " << unresolvedClusters.size();
                // Resolve MDB clusters
                TDatabaseResolverResponse::TDatabaseDescriptionMap descriptions;

                for (const auto& [databaseIdWithType, databaseAuth] : unresolvedClusters) {
                    // Now it's only possible to emit a single request with a single cluster ID simultaneously.
                    // FIXME: use batch async handling after YQ-2536 is fixed.
                    IDatabaseAsyncResolver::TDatabaseAuthMap request;
                    request[databaseIdWithType] = databaseAuth;
                    auto response = State_->DatabaseResolver->ResolveIds(request).GetValueSync();

                    if (!response.Success) {
                        ctx.IssueManager.AddIssues(response.Issues);
                        return TStatus::Error;
                    }

                    for (const auto& [databaseIdWithType, databaseDescription] : response.DatabaseDescriptionMap) {
                        YQL_CLOG(INFO, ProviderGeneric) << "resolved database id into endpoint"
                                                        << ": databaseId=" << databaseIdWithType.first
                                                        << ", databaseKind=" << databaseIdWithType.second
                                                        << ", host=" << databaseDescription.Host
                                                        << ", port=" << databaseDescription.Port;
                    }

                    // save ids for the further use
                    DatabaseDescriptions_.insert(response.DatabaseDescriptionMap.cbegin(),
                                                 response.DatabaseDescriptionMap.cend());
                }

                auto promise = NThreading::NewPromise<void>();
                promise.SetValue();
                AsyncFuture_ = promise.GetFuture();

                return TStatus::Async;
            }

            NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode&) final {
                return AsyncFuture_;
            }

            TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
                output = input;
                AsyncFuture_.GetValue();

                // Modify cluster configs with resolved ids
                auto status = ModifyClusterConfigs(DatabaseDescriptions_, ctx);

                // Clear results map
                if (!DatabaseDescriptions_.empty()) {
                    DatabaseDescriptions_ = {};
                }

                return status;
            }

            void Rewind() final {
                AsyncFuture_ = {};
                // Clear results map
                DatabaseDescriptions_.clear();
            }

        private:
            TStatus ModifyClusterConfigs(const TDatabaseResolverResponse::TDatabaseDescriptionMap& databaseDescriptions, TExprContext& ctx) {
                const auto& databaseIdsToClusterNames = State_->Configuration->DatabaseIdsToClusterNames;
                auto& clusterNamesToClusterConfigs = State_->Configuration->ClusterNamesToClusterConfigs;

                for (const auto& [databaseIdWithType, databaseDescription] : databaseDescriptions) {
                    const auto& databaseId = databaseIdWithType.first;

                    Y_ENSURE(databaseDescription.Host, "Empty resolved database host");
                    Y_ENSURE(databaseDescription.Port, "Empty resolved database port");

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
                            issues.AddIssue(TStringBuilder() << "no cluster names for database id "
                                                             << databaseIdWithType.first
                                                             << " and cluster name "
                                                             << clusterName);
                            ctx.IssueManager.AddIssues(issues);
                            return TStatus::Error;
                        }

                        auto endpointDst = clusterConfigIter->second.mutable_endpoint();
                        endpointDst->set_host(databaseDescription.Host);
                        endpointDst->set_port(databaseDescription.Port);

                        // If we work with managed YDB, we find out database name
                        // only after database id (== cluster id) resolving.
                        if (clusterConfigIter->second.kind() == NConnector::NApi::EDataSourceKind::YDB) {
                            clusterConfigIter->second.set_databasename(databaseDescription.Database);
                        }

                        YQL_CLOG(INFO, ProviderGeneric) << "ModifyClusterConfigs: "
                                                        << DumpGenericClusterConfig(clusterConfigIter->second);
                    }
                }

                return TStatus::Ok;
            }

            const TGenericState::TPtr State_;
            TDatabaseResolverResponse::TDatabaseDescriptionMap DatabaseDescriptions_;
            NThreading::TFuture<void> AsyncFuture_;
        };
    }

    THolder<IGraphTransformer> CreateGenericIODiscoveryTransformer(TGenericState::TPtr state) {
        return THolder(new TGenericIODiscoveryTransformer(std::move(state)));
    }

}
