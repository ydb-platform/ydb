#include "yql_solomon_provider_impl.h"

#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/solomon/common/util.h>
#include <ydb/library/yql/providers/solomon/expr_nodes/yql_solomon_expr_nodes.h>
#include <ydb/library/yql/providers/solomon/solomon_accessor/client/solomon_accessor_client.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

namespace NYql {

using namespace NNodes;

namespace {

struct TLoadSolomonMetaRequest {
    NSo::ISolomonAccessorClient::TPtr SolomonClient;
    NThreading::TFuture<NSo::TGetLabelsResponse> LabelNamesRequest;
    NThreading::TFuture<NSo::TListMetricsResponse> ListMetricsRequest;
};

TMaybe<TString> ExtractSetting(const TExprNode& settings, const TString& settingName) {
    for (size_t i = 0U; i < settings.ChildrenSize(); ++i) {
        if (settings.Child(i)->Head().IsAtom(settingName)) {
            return TString(settings.Child(i)->Tail().Content());
        }
    }

    return {};
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

        std::vector<NThreading::TFuture<void>> futures;
        futures.reserve(nodes.size() * 2);
        for (const auto& n : nodes) {
            TSoReadObject soReadObject(n);

            const auto& clusterName = soReadObject.DataSource().Cluster().StringValue();
            const auto* clusterDesc = State_->Configuration->ClusterConfigs.FindPtr(clusterName);
            auto& settings = soReadObject.Object().Settings().Ref();

            if (auto maybeSelectors = ExtractSetting(settings, "selectors")) {
                NSo::NProto::TDqSolomonSource source = NSo::FillSolomonSource(clusterDesc, soReadObject.Object().Project().StringValue());
                
                auto selectors = NSo::ExtractSelectorValues(*maybeSelectors);
                if (source.GetClusterType() == NSo::NProto::CT_MONITORING) {
                    selectors["cluster"] = source.GetCluster();
                    selectors["service"] = soReadObject.Object().Project().StringValue();
                } else {
                    selectors["project"] = source.GetProject();
                }

                auto defaultReplica = (source.GetClusterType() == NSo::NProto::CT_SOLOMON ? "sas" : "cloud-prod-a");
                auto solomonClientDefaultReplica = State_->Configuration->SolomonClientDefaultReplica.Get().OrElse(defaultReplica);
                source.MutableSettings()->insert({ "solomonClientDefaultReplica", ToString(solomonClientDefaultReplica) });

                auto providerFactory = CreateCredentialsProviderFactoryForStructuredToken(State_->CredentialsFactory, State_->Configuration->Tokens.at(clusterName));
                auto credentialsProvider = providerFactory->CreateProvider();

                auto solomonClient = NSo::ISolomonAccessorClient::Make(std::move(source), credentialsProvider);
                auto labelNamesFuture = solomonClient->GetLabelNames(selectors);
                auto listMetricsFuture = solomonClient->ListMetrics(selectors, 30, 0);

                LabelNamesRequests_[soReadObject.Raw()] = {
                    .SolomonClient = solomonClient,
                    .LabelNamesRequest = labelNamesFuture,
                    .ListMetricsRequest = listMetricsFuture
                };

                futures.push_back(labelNamesFuture.IgnoreResult());
                futures.push_back(listMetricsFuture.IgnoreResult());
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

        TNodeMap<TLoadSolomonMetaRequest> labelNamesRequests;
        labelNamesRequests.swap(LabelNamesRequests_);

        TNodeOnNodeOwnedMap replaces;
        for (auto& [node, request] : labelNamesRequests) {
            auto labelNamesValue = request.LabelNamesRequest.GetValue();
            if (labelNamesValue.Status != NSo::EStatus::STATUS_OK) {
                ctx.AddError(TIssue(ctx.GetPosition(node->Pos()),
                        TStringBuilder() << "Failed to get label names, details: " << labelNamesValue.Error));
                return TStatus::Error;
            }

            auto listMetricsValue = request.ListMetricsRequest.GetValue();
            if (listMetricsValue.Status != NSo::EStatus::STATUS_OK) {
                ctx.AddError(TIssue(ctx.GetPosition(node->Pos()),
                        TStringBuilder() << "Failed to get total metrics count, details: " << listMetricsValue.Error));
                return TStatus::Error;
            }

            TSoReadObject read(node);
            TVector<TCoAtom> labelNames;
            for (const auto& label : labelNamesValue.Result.Labels) {
                labelNames.push_back(Build<TCoAtom>(ctx, read.Pos()).Value(label).Done());
            }

            replaces.emplace(node,
                Build<TSoReadObject>(ctx, read.Pos())
                    .InitFrom(read)
                    .RequiredLabelNames()
                        .Add(labelNames)
                        .Build()
                    .TotalMetricsCount<TCoAtom>().Build(ToString(listMetricsValue.Result.TotalCount))
                .Done().Ptr());
        }

        return RemapExpr(input, output, replaces, ctx, TOptimizeExprSettings{nullptr});
    }

    void Rewind() final {
    }

private:
    TSolomonState::TPtr State_;
    NThreading::TFuture<void> AllFuture_;

    TNodeMap<TLoadSolomonMetaRequest> LabelNamesRequests_;
};

THolder<IGraphTransformer> CreateSolomonLoadTableMetadataTransformer(TSolomonState::TPtr state) {
    return THolder(new TSolomonLoadTableMetadataTransformer(state));
}

} // namespace NYql
