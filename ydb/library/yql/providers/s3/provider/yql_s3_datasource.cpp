#include "yql_s3_provider_impl.h"
#include "yql_s3_dq_integration.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/common/config/yql_configuration_transformer.h>
#include <ydb/library/yql/providers/common/config/yql_setting.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/structured_token/yql_token_builder.h>
#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>
#include <ydb/library/yql/providers/s3/proto/credentials.pb.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

class TS3DataSourceProvider : public TDataProviderBase {
public:
    TS3DataSourceProvider(TS3State::TPtr state)
        : State_(std::move(state))
        , IODiscoveryTransformer_(CreateS3IODiscoveryTransformer(State_))
        , ConfigurationTransformer_(MakeHolder<NCommon::TProviderConfigurationTransformer>(State_->Configuration, *State_->Types, TString{S3ProviderName}))
        , CallableExecutionTransformer_(CreateS3SourceCallableExecutionTransformer(State_))
        , TypeAnnotationTransformer_(CreateS3DataSourceTypeAnnotationTransformer(State_))
        , DqIntegration_(CreateS3DqIntegration(State_))
    {}

    void AddCluster(const TString& name, const THashMap<TString, TString>& properties) override {
        State_->Configuration->AddValidCluster(name);
        auto& settings = State_->Configuration->Clusters[name];
        settings.Url = properties.Value("location", "");
        if (!settings.Url.EndsWith("/")) {
            settings.Url += "/";
        }
        auto authMethod = properties.Value("authMethod", "");
        if (authMethod == "SERVICE_ACCOUNT") {
            State_->Configuration->Tokens[name] = ComposeStructuredTokenJsonForServiceAccountWithSecret(properties.Value("serviceAccountId", ""), properties.Value("serviceAccountIdSignatureReference", ""), properties.Value("serviceAccountIdSignature", ""));
            return;
        }

        if (authMethod == "AWS") {
            NS3::TAwsParams params;
            params.SetAwsAccessKey(properties.Value("awsAccessKeyId", ""));
            params.SetAwsRegion(properties.Value("awsRegion", ""));
            State_->Configuration->Tokens[name] = ComposeStructuredTokenJsonForBasicAuthWithSecret(params.SerializeAsString(), properties.Value("awsSecretAccessKeyReference", ""), properties.Value("awsSecretAccessKey", ""));
            return;
        }

        State_->Configuration->Tokens[name] = ComposeStructuredTokenJsonForServiceAccount(properties.Value("serviceAccountId", ""), properties.Value("serviceAccountIdSignature", ""), properties.Value("authToken", ""));
    }

    TStringBuf GetName() const override {
        return S3ProviderName;
    }

    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        if (node.IsCallable(TCoDataSource::CallableName())) {
            if (node.Head().Content() == S3ProviderName) {
                if (const auto& clusterName = node.Tail().Content(); NCommon::ALL_CLUSTERS != clusterName && !State_->Configuration->HasCluster(clusterName)) {
                    ctx.AddError(TIssue(ctx.GetPosition(node.Tail().Pos()), TStringBuilder() <<
                        "Unknown s3 cluster name: " << clusterName));
                    return false;
                } else {
                    cluster = clusterName;
                    return true;
                }
            }
        }
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Invalid S3 DataSource parameters"));
        return false;
    }

    bool CanParse(const TExprNode& node) override {
        if (node.IsCallable(TCoRead::CallableName())) {
            return NNodes::TS3DataSource::Match(node.Child(1));
        }
        return TypeAnnotationTransformer_->CanParse(node);
    }

    IGraphTransformer& GetIODiscoveryTransformer() override {
        return *IODiscoveryTransformer_;
    }

    IGraphTransformer& GetConfigurationTransformer() override {
        return *ConfigurationTransformer_;
    }

    IGraphTransformer& GetTypeAnnotationTransformer(bool instantOnly) override {
        Y_UNUSED(instantOnly);
        return *TypeAnnotationTransformer_;
    }

    IGraphTransformer& GetCallableExecutionTransformer() override {
        return *CallableExecutionTransformer_;
    }

    TExprNode::TPtr RewriteIO(const TExprNode::TPtr& node, TExprContext& ctx) override {
        Y_UNUSED(ctx);
        YQL_CLOG(INFO, ProviderS3) << "RewriteIO";
        return node;
    }

    bool CanPullResult(const TExprNode& node, TSyncMap& syncList, bool& canRef) override {
        Y_UNUSED(syncList);
        canRef = false;
        if (node.IsCallable(TCoRight::CallableName())) {
            const auto input = node.Child(0);
            if (input->IsCallable(TS3ReadObject::CallableName())) {
                return true;
            }
        }
        return false;
    }

    bool CanExecute(const TExprNode& node) override {
        if (node.IsCallable(TS3ReadObject::CallableName())) {
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

        if (TMaybeNode<TS3ReadObject>(&node)) {
            return true;
        }
        return false;
    }

    ui32 GetInputs(const TExprNode& node, TVector<TPinInfo>&, bool withLimits) override {
        Y_UNUSED(withLimits);
        if (auto maybeRead = TMaybeNode<TS3ReadObject>(&node)) {
        }
        return 0;
    }

    IDqIntegration* GetDqIntegration() override {
        return DqIntegration_.Get();
    }
private:
    const TS3State::TPtr State_;
    const THolder<IGraphTransformer> IODiscoveryTransformer_;
    const THolder<IGraphTransformer> ConfigurationTransformer_;
    const THolder<IGraphTransformer> CallableExecutionTransformer_;
    const THolder<TVisitorTransformerBase> TypeAnnotationTransformer_;
    const THolder<IDqIntegration> DqIntegration_;
};

}

TIntrusivePtr<IDataProvider> CreateS3DataSource(TS3State::TPtr state) {
    return new TS3DataSourceProvider(std::move(state));
}

} // namespace NYql
