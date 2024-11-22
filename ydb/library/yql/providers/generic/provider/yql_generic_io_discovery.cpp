#include "yql_generic_provider_impl.h"
#include "yql_generic_utils.h"

#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql {

    namespace {

        using namespace NNodes;

        // IClusterConfigModifier provides interface for modifying generic cluster configs 
        // that describe connections to the cloud-based resources.
        // Since they do not have information about network endpoints, we need to resolve and
        // add them to the configs explicitly.
        class IClusterConfigModifier {
        public:
            using TPtr = std::unique_ptr<IClusterConfigModifier>;

            using TFutures = TVector<NThreading::TFuture<TIssues>>;

            virtual void CollectUnresolvedClusters(const TExprNode::TListType& reads) = 0;
            virtual TFutures ResolveClusters() = 0;
            virtual TIssues ModifyClusterConfigs() = 0;
            virtual std::size_t Count() const = 0;
            virtual void Cleanup() = 0;
            virtual ~IClusterConfigModifier() = default;
        };

        class TManagedDatabasesConfigModifier: public IClusterConfigModifier {
        public:
            TManagedDatabasesConfigModifier() = delete;

            TManagedDatabasesConfigModifier(const TGenericState::TPtr& state): State_(state) {};

            void CollectUnresolvedClusters(const TExprNode::TListType& reads) override {
                for (auto& node : reads) {
                    const TGenRead read(node);
                    const auto clusterName = read.DataSource().Cluster().StringValue();
                    const auto& clusterConfig = State_->Configuration->ClusterNamesToClusterConfigs[clusterName];

                    // resolve managed databases that have database_id identifier.
                    auto databaseId = clusterConfig.GetDatabaseId();
                    if (databaseId) {
                        YQL_CLOG(DEBUG, ProviderGeneric) << "discovered managed database external data source: "
                            << "clusterName=" << clusterName 
                            << ", databaseId=" << databaseId;
                        const auto idKey = std::make_pair(databaseId, DatabaseTypeFromDataSourceKind(clusterConfig.GetKind()));
                        const auto iter = State_->DatabaseAuth.find(idKey);
                        if (iter != State_->DatabaseAuth.cend()) {
                            YQL_CLOG(DEBUG, ProviderGeneric) << "requesting to resolve"
                                                             << ": clusterName=" << clusterName
                                                             << ", databaseId=" << databaseId;

                            ManagedDatabases_[idKey] = iter->second;
                        }
                    }
                }

                return;
            }

            virtual TFutures ResolveClusters() override {
                TFutures futures;

                for (const auto& [databaseIdWithType, databaseAuth] : ManagedDatabases_) {
                    auto promise = NThreading::NewPromise<TIssues>();

                    // Now it's only possible to emit a single request with a single cluster ID simultaneously.
                    // FIXME: use batch async handling after YQ-2536 is fixed.
                    IDatabaseAsyncResolver::TDatabaseAuthMap request;
                    request[databaseIdWithType] = databaseAuth;
                    auto response = State_->DatabaseResolver->ResolveIds(request).GetValueSync();

                    if (!response.Success) {
                        promise.SetValue(std::move(response.Issues));
                    } else {
                        for (const auto& [databaseIdWithType, databaseDescription] : response.DatabaseDescriptionMap) {
                            YQL_CLOG(INFO, ProviderGeneric) << "resolved database id into endpoint"
                                                            << ": databaseId=" << databaseIdWithType.first
                                                            << ", kind=" << databaseIdWithType.second
                                                            << ", host=" << databaseDescription.Host
                                                            << ", port=" << databaseDescription.Port;
                        }

                        // save ids for the further use
                        DatabaseDescriptions_.insert(response.DatabaseDescriptionMap.cbegin(),
                                                    response.DatabaseDescriptionMap.cend());

                        promise.SetValue({});
                    }

                    futures.emplace_back(promise.GetFuture());
                }

                return futures;
            }

            TIssues ModifyClusterConfigs() override {
                TIssues issues;
                const auto& databaseIdsToClusterNames = State_->Configuration->DatabaseIdsToClusterNames;
                auto& clusterNamesToClusterConfigs = State_->Configuration->ClusterNamesToClusterConfigs;

                for (const auto& [databaseIdWithType, databaseDescription] : DatabaseDescriptions_) {
                    const auto& databaseId = databaseIdWithType.first;

                    Y_ENSURE(databaseDescription.Host, "Empty resolved database host");
                    Y_ENSURE(databaseDescription.Port, "Empty resolved database port");

                    auto clusterNamesIter = databaseIdsToClusterNames.find(databaseId);

                    if (clusterNamesIter == databaseIdsToClusterNames.cend()) {
                        issues.AddIssue(TStringBuilder() << "no cluster names for database id " << databaseId);
                        return issues;
                    }

                    for (const auto& clusterName : clusterNamesIter->second) {
                        auto clusterConfigIter = clusterNamesToClusterConfigs.find(clusterName);

                        if (clusterConfigIter == clusterNamesToClusterConfigs.end()) {
                            TIssues issues;
                            issues.AddIssue(TStringBuilder() << "no cluster names for database id "
                                                             << databaseIdWithType.first
                                                             << " and cluster name "
                                                             << clusterName);
                            return issues;
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

                return issues;
            }

            void Cleanup() override {
                if (!DatabaseDescriptions_.empty()) {
                    DatabaseDescriptions_ = {};
                }
            }

            std::size_t Count() const override {
                return DatabaseDescriptions_.size();
            }

        private:
            IDatabaseAsyncResolver::TDatabaseAuthMap ManagedDatabases_;
            TDatabaseResolverResponse::TDatabaseDescriptionMap DatabaseDescriptions_;
            const TGenericState::TPtr& State_;
        };

        class TLoggingConfigModifier: public IClusterConfigModifier {
        public:
            TLoggingConfigModifier(const TGenericState::TPtr& state)
                : State_(state) {}

            virtual void CollectUnresolvedClusters(const TExprNode::TListType& reads) override {
                for (auto& node : reads) {
                    const TGenRead read(node);
                    const auto clusterName = read.DataSource().Cluster().StringValue();
                    const auto& cluster = State_->Configuration->ClusterNamesToClusterConfigs[clusterName];

                    if (cluster.GetKind() == NYql::NConnector::NApi::EDataSourceKind::LOGGING) {
                        const auto& folderId = cluster.datasourceoptions().at("folder_id");
                        const auto& keyArg = TExprBase(read.FreeArgs().Get(2).Ref().HeadPtr()).Cast<TCoKey>().Ref().Head();
                        const auto logGroupName = TString(keyArg.Tail().Head().Content());

                        const auto iter = State_->LoggingAuth.find(folderId);
                        if (iter != State_->LoggingAuth.cend()) {
                            YQL_CLOG(DEBUG, ProviderGeneric) << "discovered logging external data source: "
                                << "clusterName=" << clusterName 
                                << ", folderId=" << folderId 
                                << ", logGroupName=" << logGroupName;

                            UnresolvedItems_[clusterName] = {
                                .FolderId = folderId,
                                .LogGroupName = logGroupName,
                                .Auth = iter->second,
                            };
                        }
                    }
                }

                return;
            }

            virtual TFutures ResolveClusters() override {
                TFutures futures;

                for (const auto& [clusterId, auth] : UnresolvedItems_) {
                    auto responseFuture = State_->LoggingResolver->Resolve(ILoggingResolver::TRequest{
                        .FolderId = "",
                        .LogGroupName = "",
                    });

                    auto issueFuture = responseFuture.Apply(
                        [this, clusterId, responseFuture](const NThreading::TFuture<ILoggingResolver::TResponse>&) mutable {

                        auto response = responseFuture.ExtractValue();

                        if (response.Issues.Empty()) {
                            std::lock_guard<std::mutex> guard(ResolvedItemsMutex_);
                            ResolvedItems_[clusterId] = {
                                .FolderId = "",
                                .LogGroupName = "",
                                .Host = response.Host,
                                .Port = response.Port,
                                .Table = response.Table,
                            };
                        }

                        return response.Issues;
                    });

                    futures.emplace_back(std::move(issueFuture));
                }

                return futures;
            };

            virtual TIssues ModifyClusterConfigs() override {
                TIssues issues;

                auto& clusterNamesToClusterConfigs = State_->Configuration->ClusterNamesToClusterConfigs;

                for (auto& [clusterName, clusterConfig] : clusterNamesToClusterConfigs) {
                    auto itemIter = ResolvedItems_.find(clusterName);
                    if (itemIter == ResolvedItems_.cend()) {
                        issues.AddIssue(TIssue{TStringBuilder() << "no resolved item for cluster " << clusterName});
                        continue;
                    }

                    clusterConfig.mutable_endpoint()->set_host(itemIter->second.Host);
                    clusterConfig.mutable_endpoint()->set_port(itemIter->second.Port);
                    clusterConfig.mutable_datasourceoptions()->insert({"table", itemIter->second.Table});
                }

                return issues;
            };

            virtual std::size_t Count() const override {
                return UnresolvedItems_.size();
            };

            virtual void Cleanup() override {
                UnresolvedItems_.clear();
                ResolvedItems_.clear();
            };

        private:
            struct TUnresolvedItem {
                TString FolderId;
                TString LogGroupName;
                ILoggingResolver::TAuth Auth;
            };

            // cluster_name -> unresolved item
            THashMap<TString, TUnresolvedItem> UnresolvedItems_;

            struct TResolvedItem {
                TString FolderId;
                TString LogGroupName; 
                TString Host;
                ui32    Port;
                TString Table;
            };

            std::mutex ResolvedItemsMutex_;

            // cluster_name -> resolved item
            THashMap<TString, TResolvedItem> ResolvedItems_;

            const TGenericState::TPtr& State_;
        };

        class TGenericIODiscoveryTransformer: public TGraphTransformerBase {
        public:
            TGenericIODiscoveryTransformer(TGenericState::TPtr state)
                : State_(std::move(state))
                , ManagedDatabasesConfigModifier_(std::make_unique<TManagedDatabasesConfigModifier>(State_))
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

                // Collect clusters that need to be resolved
                ManagedDatabasesConfigModifier_->CollectUnresolvedClusters(reads);
                YQL_CLOG(DEBUG, ProviderGeneric) << "total database clusters to be resolved: " << ManagedDatabasesConfigModifier_->Count();

                // Resolve clusters
                auto futures = ManagedDatabasesConfigModifier_->ResolveClusters();

                // Block until all futures are ready
                TIssues issues;
                for (auto& future : futures) {
                    issues.AddIssues(future.GetValueSync());
                }

                if (!issues.Empty()) {
                    ctx.IssueManager.AddIssues(issues);
                    return TStatus::Error;
                }

                // We're done
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
                auto issues = ManagedDatabasesConfigModifier_->ModifyClusterConfigs();

                // Clear results map
                ManagedDatabasesConfigModifier_->Cleanup();

                if (!issues.Empty()) {
                    ctx.IssueManager.AddIssues(issues);
                    return TStatus::Error;
                }

                return TStatus::Ok;
            }

            void Rewind() final {
                AsyncFuture_ = {};
                // Clear results map
                ManagedDatabasesConfigModifier_->Cleanup();
            }

        private:
            const TGenericState::TPtr State_;
            IClusterConfigModifier::TPtr ManagedDatabasesConfigModifier_;
            NThreading::TFuture<void> AsyncFuture_;
        };
    } // namespace

    THolder<IGraphTransformer> CreateGenericIODiscoveryTransformer(TGenericState::TPtr state) {
        return THolder(new TGenericIODiscoveryTransformer(std::move(state)));
    }

} // namespace NYql
