#include "yql_solomon_provider_impl.h"
#include "yql_solomon_dq_integration.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/solomon/expr_nodes/yql_solomon_expr_nodes.h>

#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/common/config/yql_configuration_transformer.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

class TSolomonDataSource : public TDataProviderBase {
public:
    TSolomonDataSource(TSolomonState::TPtr state)
        : State_(state)
        , ConfigurationTransformer_(NCommon::CreateProviderConfigurationTransformer(
            State_->Configuration, *State_->Types, TString{SolomonProviderName}))
        , IODiscoveryTransformer_(CreateSolomonIODiscoveryTransformer(State_))
        , LoadMetaDataTransformer_(CreateSolomonLoadTableMetadataTransformer(State_))
        , TypeAnnotationTransformer_(CreateSolomonDataSourceTypeAnnotationTransformer(State_))
        , ExecutionTransformer_(CreateSolomonDataSourceExecTransformer(State_))
        , DqIntegration_(CreateSolomonDqIntegration(State_))
    {
    }

    TStringBuf GetName() const override {
        return SolomonProviderName;
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

    IGraphTransformer& GetCallableExecutionTransformer() override {
        return *ExecutionTransformer_;
    }

    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        if (node.IsCallable(TCoDataSource::CallableName())) {
            if (node.Child(0)->Content() == SolomonProviderName) {
                auto clusterName = node.Child(1)->Content();
                if (!State_->Gateway->HasCluster(clusterName)) {
                    ctx.AddError(TIssue(ctx.GetPosition(node.Child(1)->Pos()), TStringBuilder() <<
                        "Unknown cluster name: " << clusterName));
                    return false;
                }
                cluster = clusterName;
                return true;
            }
        }
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Invalid Solomon DataSource parameters"));
        return false;
    }

    bool CanParse(const TExprNode& node) override {
        if (node.IsCallable(TCoRead::CallableName())) {
            return TSoDataSource::Match(node.Child(1));
        }
        return TypeAnnotationTransformer_->CanParse(node);
    }

    bool CanExecute(const TExprNode& node) override {
        return ExecutionTransformer_->CanExec(node);
    }

    bool CanPullResult(const TExprNode& node, TSyncMap& syncList, bool& canRef) override {
        Y_UNUSED(syncList);
        canRef = false;
        if (node.IsCallable(TCoRight::CallableName())) {
            const auto input = node.Child(0);
            if (input->IsCallable(TSoReadObject::CallableName())) {
                return true;
            }
        }
        return false;
    }

    TExprNode::TPtr RewriteIO(const TExprNode::TPtr& node, TExprContext& ctx) override {
        Y_UNUSED(ctx);
        YQL_CLOG(INFO, ProviderSolomon) << "RewriteIO";
        return node;
    }

    bool GetDependencies(const TExprNode& node, TExprNode::TListType& children, bool compact) override {
        Y_UNUSED(compact);

        for (auto& child : node.Children()) {
            children.push_back(child.Get());
        }

        if (TMaybeNode<TSoReadObject>(&node)) {
            return true;
        }
        return false;
    }

    ui32 GetInputs(const TExprNode& node, TVector<TPinInfo>&, bool withLimits) override {
        Y_UNUSED(node);
        Y_UNUSED(withLimits);
        return 0;
    }

    IDqIntegration* GetDqIntegration() override {
        return DqIntegration_.Get();
    }

private:
    TSolomonState::TPtr State_;

    THolder<IGraphTransformer> ConfigurationTransformer_;
    THolder<IGraphTransformer> IODiscoveryTransformer_;
    THolder<IGraphTransformer> LoadMetaDataTransformer_;
    THolder<TVisitorTransformerBase> TypeAnnotationTransformer_;
    THolder<TExecTransformerBase> ExecutionTransformer_;
    const THolder<IDqIntegration> DqIntegration_;
};

TIntrusivePtr<IDataProvider> CreateSolomonDataSource(TSolomonState::TPtr state) {
    return new TSolomonDataSource(state);
}

} // namespace NYql
