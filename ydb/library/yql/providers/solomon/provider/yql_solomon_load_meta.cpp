#include "yql_solomon_provider_impl.h"

#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/solomon/expr_nodes/yql_solomon_expr_nodes.h>
#include <ydb/library/yql/providers/solomon/solomon_accessor/client/solomon_accessor_client.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

namespace NYql {

using namespace NNodes;

namespace {

TMaybe<TString> ExtractSetting(const TExprNode& settings, const TString& settingName) {
    for (size_t i = 0U; i < settings.ChildrenSize(); ++i) {
        if (settings.Child(i)->Head().IsAtom(settingName)) {
            return TString(settings.Child(i)->Tail().Content());
        }
    }

    return {};
}

NSo::NProto::ESolomonClusterType MapClusterType(TSolomonClusterConfig::ESolomonClusterType clusterType) {
    switch (clusterType) {
        case TSolomonClusterConfig::SCT_SOLOMON:
            return NSo::NProto::ESolomonClusterType::CT_SOLOMON;
        case TSolomonClusterConfig::SCT_MONITORING:
            return NSo::NProto::ESolomonClusterType::CT_MONITORING;
        default:
            YQL_ENSURE(false, "Invalid cluster type " << ToString<ui32>(clusterType));
    }
}

} // namespace

class TSolomonLoadTableMetadataTransformer : public TGraphTransformerBase {
public:
    TSolomonLoadTableMetadataTransformer(TSolomonState::TPtr state)
        : State_(state)
    {
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;
        if (ctx.Step.IsDone(TExprStep::LoadTablesMetadata)) {
            return TStatus::Ok;
        }

        auto nodes = FindNodes(input, [&](const TExprNode::TPtr& node) -> bool {
            if (const auto maybeRead = TMaybeNode<TSoReadObject>(node)) {
                TSoReadObject read(node);
                auto& settings = read.Object().Settings().Ref();
                if (read.RequiredLabelNames().Empty() && ExtractSetting(settings, "selectors")) {
                    return true;
                }
            }
            return false;
        });

        std::vector<NThreading::TFuture<NSo::TGetLabelsResponse>> futures;
        futures.reserve(nodes.size());
        for (const auto& n : nodes) {
            TSoReadObject soReadObject(n);

            const auto& clusterName = soReadObject.DataSource().Cluster().StringValue();
            const auto* clusterDesc = State_->Configuration->ClusterConfigs.FindPtr(clusterName);
            auto& settings = soReadObject.Object().Settings().Ref();

            if (auto maybeSelectors = ExtractSetting(settings, "selectors")) {
                NSo::NProto::TDqSolomonSource source;
                source.SetEndpoint(clusterDesc->GetCluster());
                source.SetProject(soReadObject.Object().Project().StringValue());
                source.SetClusterType(MapClusterType(clusterDesc->GetClusterType()));
                source.SetUseSsl(clusterDesc->GetUseSsl());

                auto defaultReplica = State_->Configuration->SolomonClientDefaultReplica.Get().OrElse("sas");
                source.MutableSettings()->insert({ "solomonClientDefaultReplica", ToString(defaultReplica) });

                auto providerFactory = CreateCredentialsProviderFactoryForStructuredToken(State_->CredentialsFactory, State_->Configuration->Tokens.at(clusterName));
                auto credentialsProvider = providerFactory->CreateProvider();

                SolomonClient_ = NSo::ISolomonAccessorClient::Make(std::move(source), credentialsProvider);
                auto future = SolomonClient_->GetLabelNames(*maybeSelectors);

                LabelNamesRequests_[soReadObject.Raw()] = future;
                futures.push_back(future);
            }
        }

        if (futures.empty()) {
            return TStatus::Ok;
        }

        AllFuture_ = NThreading::WaitExceptionOrAll(futures);
        return TStatus::Async;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode&) final {
        return AllFuture_;
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        AllFuture_.GetValue();

        TNodeMap<NThreading::TFuture<NSo::TGetLabelsResponse>> labelNamesRequests;
        labelNamesRequests.swap(LabelNamesRequests_);

        TNodeOnNodeOwnedMap replaces;
        for (auto& [node, request] : labelNamesRequests) {
            auto value = request.GetValue();

            if (value.Status != NSo::EStatus::STATUS_OK) {
                ctx.AddError(TIssue(ctx.GetPosition(node->Pos()),
                        TStringBuilder() << "Failed to get label names, details: " << value.Error));
                return TStatus::Error;
            }

            TSoReadObject read(node);
            TVector<TCoAtom> labelNames;
            for (const auto& label : value.Result.Labels) {
                labelNames.push_back(Build<TCoAtom>(ctx, read.Pos()).Value(label).Done());
            }

            replaces.emplace(node,
                Build<TSoReadObject>(ctx, read.Pos())
                    .InitFrom(read)
                    .RequiredLabelNames()
                        .Add(labelNames)
                        .Build()
                .Done().Ptr());
        }

        return RemapExpr(input, output, replaces, ctx, TOptimizeExprSettings{nullptr});
    }

    void Rewind() final {
    }

private:
    TSolomonState::TPtr State_;
    NThreading::TFuture<void> AllFuture_;

    NSo::ISolomonAccessorClient::TPtr SolomonClient_;
    TNodeMap<NThreading::TFuture<NSo::TGetLabelsResponse>> LabelNamesRequests_;
};

THolder<IGraphTransformer> CreateSolomonLoadTableMetadataTransformer(TSolomonState::TPtr state) {
    return THolder(new TSolomonLoadTableMetadataTransformer(state));
}

} // namespace NYql
