#include "yql_generic_cluster_config.h"
#include "yql_generic_dq_integration.h"
#include "yql_generic_provider_impl.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/common/config/yql_configuration_transformer.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/client.h>
#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>
#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

    using namespace NNodes;

    namespace {

        class TGenericDataSource: public TDataProviderBase {
        public:
            TGenericDataSource(TGenericState::TPtr state)
                : State_(state)
                , ConfigurationTransformer_(MakeHolder<NCommon::TProviderConfigurationTransformer>(State_->Configuration, *State_->Types, TString{GenericProviderName}))
                , IODiscoveryTransformer_(CreateGenericIODiscoveryTransformer(State_))
                , LoadMetaDataTransformer_(CreateGenericLoadTableMetadataTransformer(State_))
                , TypeAnnotationTransformer_(CreateGenericDataSourceTypeAnnotationTransformer(State_))
                , DqIntegration_(CreateGenericDqIntegration(State_))
            {
            }

            TStringBuf GetName() const override {
                return GenericProviderName;
            }

            bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
                if (node.IsCallable(TCoDataSource::CallableName())) {
                    if (node.Child(0)->Content() == GenericProviderName) {
                        auto clusterName = node.Child(1)->Content();
                        if (clusterName != NCommon::ALL_CLUSTERS && !State_->Configuration->HasCluster(clusterName)) {
                            ctx.AddError(TIssue(ctx.GetPosition(node.Child(1)->Pos()),
                                                TStringBuilder() << "Unknown cluster name: " << clusterName));
                            return false;
                        }
                        cluster = clusterName;
                        return true;
                    }
                }
                ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Invalid Generic DataSource parameters"));
                return false;
            }

            bool CanParse(const TExprNode& node) override {
                if (node.IsCallable(TCoRead::CallableName())) {
                    return TGenDataSource::Match(node.Child(1));
                }
                return TypeAnnotationTransformer_->CanParse(node);
            }

            IGraphTransformer& GetConfigurationTransformer() override {
                return *ConfigurationTransformer_;
            }

            IGraphTransformer& GetIODiscoveryTransformer() override {
                return *IODiscoveryTransformer_;
            }

            IGraphTransformer& GetLoadTableMetadataTransformer() override {
                return *LoadMetaDataTransformer_;
            }

            IGraphTransformer& GetTypeAnnotationTransformer(bool instantOnly) override {
                Y_UNUSED(instantOnly);
                return *TypeAnnotationTransformer_;
            }

            TExprNode::TPtr RewriteIO(const TExprNode::TPtr& node, TExprContext& ctx) override {
                Y_UNUSED(ctx);
                YQL_CLOG(INFO, ProviderGeneric) << "RewriteIO";
                return node;
            }

            bool CanPullResult(const TExprNode& node, TSyncMap& syncList, bool& canRef) override {
                Y_UNUSED(syncList);
                canRef = false;
                if (node.IsCallable(TCoRight::CallableName())) {
                    const auto input = node.Child(0);
                    if (input->IsCallable(TGenReadTable::CallableName())) {
                        return true;
                    }
                }
                return false;
            }

            bool CanExecute(const TExprNode& node) override {
                if (node.IsCallable(TGenReadTable::CallableName())) {
                    return true;
                }
                return false;
            }

            const THashMap<TString, TString>* GetClusterTokens() override {
                return &State_->Configuration->Tokens;
            }

            bool GetDependencies(const TExprNode& node, TExprNode::TListType& children, bool compact) override {
                Y_UNUSED(compact);

                for (auto& child : node.Children()) {
                    children.push_back(child.Get());
                }

                if (TMaybeNode<TGenReadTable>(&node)) {
                    return true;
                }
                return false;
            }

            ui32 GetInputs(const TExprNode& node, TVector<TPinInfo>& inputs, bool withLimits) override {
                Y_UNUSED(withLimits);
                if (auto maybeRead = TMaybeNode<TGenReadTable>(&node)) {
                    if (auto maybeTable = maybeRead.Table()) {
                        TStringBuilder tableNameBuilder;
                        if (auto dataSource = maybeRead.DataSource().Maybe<TGenDataSource>()) {
                            auto cluster = dataSource.Cast().Cluster();
                            tableNameBuilder << cluster.Value() << ".";
                        }
                        tableNameBuilder << '`' << maybeTable.Cast().Value() << '`';
                        inputs.push_back(
                            TPinInfo(maybeRead.DataSource().Raw(), nullptr, maybeTable.Cast().Raw(), tableNameBuilder, false));
                        return 1;
                    }
                }
                return 0;
            }

            IDqIntegration* GetDqIntegration() override {
                return DqIntegration_.Get();
            }

            void AddCluster(const TString& clusterName, const THashMap<TString, TString>& properties) override {
                State_->Configuration->AddCluster(
                    GenericClusterConfigFromProperties(clusterName, properties),
                    State_->DatabaseResolver,
                    State_->DatabaseAuth,
                    State_->Types->Credentials);
            }

        private:
            const TGenericState::TPtr State_;
            const THolder<IGraphTransformer> ConfigurationTransformer_;
            const THolder<IGraphTransformer> IODiscoveryTransformer_;
            const THolder<IGraphTransformer> LoadMetaDataTransformer_;
            const THolder<TVisitorTransformerBase> TypeAnnotationTransformer_;
            const THolder<IDqIntegration> DqIntegration_;
        };

    }

    TIntrusivePtr<IDataProvider> CreateGenericDataSource(TGenericState::TPtr state) {
        return new TGenericDataSource(std::move(state));
    }

} // namespace NYql
