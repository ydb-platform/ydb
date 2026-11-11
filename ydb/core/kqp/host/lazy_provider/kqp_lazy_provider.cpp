#include "kqp_lazy_provider.h"

#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>

#include <exception>
#include <memory>

namespace NKikimr::NKqp {
namespace {
using namespace NYql;

class TLazyTransformer final: public TGraphTransformerBase {
public:
    explicit TLazyTransformer(std::function<TAutoPtr<IGraphTransformer>()> factory)
        : Factory(std::move(factory))
    {
    }

private:
    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        if (!Transformer) {
            Transformer = Factory();
            Factory = {};
        }
        return Transformer->Transform(std::move(input), output, ctx);
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) override {
        return Transformer->GetAsyncFuture(input);
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        return Transformer->ApplyAsyncChanges(std::move(input), output, ctx);
    }

    void Rewind() override {
        if (Transformer) {
            Transformer->Rewind();
        }
    }

    std::function<TAutoPtr<IGraphTransformer>()> Factory;
    TAutoPtr<IGraphTransformer> Transformer;
};

struct TLazyProviderState {
    explicit TLazyProviderState(std::function<TDataProviderInfo()> factory)
        : Factory(std::move(factory))
    {
    }

    void Ensure() {
        if (Failure) {
            std::rethrow_exception(Failure);
        }
        if (!Providers.Source) {
            try {
                auto providers = Factory();
                YQL_ENSURE(providers.Source && providers.Sink);
                Providers = std::move(providers);
                Factory = {};
                for (const auto& action : PendingSourceActions) {
                    action(*Providers.Source);
                }
                for (const auto& action : PendingSinkActions) {
                    action(*Providers.Sink);
                }
                PendingSourceActions.clear();
                PendingSinkActions.clear();
            } catch (...) {
                Failure = std::current_exception();
                throw;
            }
        }
    }

    std::function<TDataProviderInfo()> Factory;
    TDataProviderInfo Providers;
    TVector<std::function<void(IDataProvider&)>> PendingSourceActions;
    TVector<std::function<void(IDataProvider&)>> PendingSinkActions;
    std::exception_ptr Failure;
};

// Layers are registered before any query is examined. Keep that registration
// lazy too, without losing layer updates that precede the first YT reference.
class TLazyLayersIntegration final: public NLayers::ILayersIntegration {
public:
    explicit TLazyLayersIntegration(std::shared_ptr<TLazyProviderState> state)
        : State(std::move(state))
    {
    }

    TMaybe<NLayers::TLocations> ResolveLayers(const TVector<NLayers::TKey>& order,
        const TString& cluster, TExprContext& ctx) const override
    {
        return Get()->ResolveLayers(order, cluster, ctx);
    }

    bool UpdateLayerCache(const NLayers::TKey& key, const NLayers::TLayerInfo& layer, TExprContext& ctx) override {
        return Get()->UpdateLayerCache(key, layer, ctx);
    }

    bool UpdateLayerLocations(const NLayers::TKey& key, NLayers::TLocations&& locations, TExprContext& ctx) override {
        return Get()->UpdateLayerLocations(key, std::move(locations), ctx);
    }

    void RemoveLayerByName(const TString& name) override {
        if (State->Providers.Source) {
            Get()->RemoveLayerByName(name);
        }
    }

private:
    NLayers::ILayersIntegrationPtr Get() const {
        State->Ensure();
        auto integration = State->Providers.Source->GetLayersIntegration();
        YQL_ENSURE(integration);
        return integration;
    }

    const std::shared_ptr<TLazyProviderState> State;
};

class TLazyProvider final: public TDataProviderBase {
public:
    TLazyProvider(TStringBuf name, TStringBuf callablePrefix, std::shared_ptr<TLazyProviderState> state,
        bool source, bool hasLayers)
        : Name(name)
        , CallablePrefix(callablePrefix)
        , State(std::move(state))
        , Source(source)
        , Layers(hasLayers ? MakeIntrusive<TLazyLayersIntegration>(State) : nullptr)
    {
    }

    TStringBuf GetName() const override {
        return Name;
    }

    bool Initialize(TExprContext&) override {
        // The factory performs initialization when this provider is first used.
        return true;
    }

    NLayers::ILayersIntegrationPtr GetLayersIntegration() const override {
        return Layers;
    }

    bool CanParse(const TExprNode& node) override {
        // Raw YQL may already contain provider callables, without a Read!/Write!
        // that would otherwise activate us through ValidateParameters first.
        const auto callable = node.Content();
        if (node.IsCallable() && callable.StartsWith(CallablePrefix) && callable.size() > CallablePrefix.size()
            && callable[CallablePrefix.size()] >= 'A' && callable[CallablePrefix.size()] <= 'Z') {
            return Get().CanParse(node);
        }
        auto* provider = Peek();
        return provider && provider->CanParse(node);
    }

    void Reset() override {
        if (auto* provider = Peek()) {
            provider->Reset();
        } else {
            PendingActions().clear();
        }
    }

    IGraphTransformer& GetConfigurationTransformer() override {
        if (auto* provider = Peek()) {
            return provider->GetConfigurationTransformer();
        }
        return TDataProviderBase::GetConfigurationTransformer();
    }

    TExprNode::TPtr GetClusterInfo(const TString& cluster, TExprContext& ctx) override {
        return Get().GetClusterInfo(cluster, ctx);
    }

    void AddCluster(const TString& name, const THashMap<TString, TString>& properties) override {
        return Get().AddCluster(name, properties);
    }

    const THashMap<TString, TString>* GetClusterTokens() override {
        return Get().GetClusterTokens();
    }

    TMaybe<TString> ResolveClusterToken(const TString& cluster) override {
        return Get().ResolveClusterToken(cluster);
    }

    const THashSet<TString>& GetValidClusters() override {
        return Get().GetValidClusters();
    }

    IGraphTransformer& GetIODiscoveryTransformer() override {
        if (auto* provider = Peek()) {
            return provider->GetIODiscoveryTransformer();
        }
        return TDataProviderBase::GetIODiscoveryTransformer();
    }

    IGraphTransformer& GetEpochsTransformer() override {
        if (auto* provider = Peek()) {
            return provider->GetEpochsTransformer();
        }
        return TDataProviderBase::GetEpochsTransformer();
    }

    IGraphTransformer& GetIntentDeterminationTransformer() override {
        if (auto* provider = Peek()) {
            return provider->GetIntentDeterminationTransformer();
        }
        return TDataProviderBase::GetIntentDeterminationTransformer();
    }

    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        return Get().ValidateParameters(node, ctx, cluster);
    }

    void FillModifyCallables(THashSet<TStringBuf>& callables) override {
        if (auto* provider = Peek()) {
            return provider->FillModifyCallables(callables);
        }
        return TDataProviderBase::FillModifyCallables(callables);
    }

    IGraphTransformer& GetTypeAnnotationTransformer(bool instantOnly) override {
        if (auto* provider = Peek()) {
            return provider->GetTypeAnnotationTransformer(instantOnly);
        }
        return TDataProviderBase::GetTypeAnnotationTransformer(instantOnly);
    }

    IGraphTransformer& GetConstraintTransformer(bool instantOnly, bool subGraph) override {
        if (auto* provider = Peek()) {
            return provider->GetConstraintTransformer(instantOnly, subGraph);
        }
        return TDataProviderBase::GetConstraintTransformer(instantOnly, subGraph);
    }

    TExprNode::TPtr RewriteIO(const TExprNode::TPtr& node, TExprContext& ctx) override {
        return Get().RewriteIO(node, ctx);
    }

    void PostRewriteIO() override {
        if (auto* provider = Peek()) {
            return provider->PostRewriteIO();
        }
        return TDataProviderBase::PostRewriteIO();
    }

    IGraphTransformer& GetRecaptureOptProposalTransformer() override {
        if (auto* provider = Peek()) {
            return provider->GetRecaptureOptProposalTransformer();
        }
        return TDataProviderBase::GetRecaptureOptProposalTransformer();
    }

    IGraphTransformer& GetStatisticsProposalTransformer() override {
        if (auto* provider = Peek()) {
            return provider->GetStatisticsProposalTransformer();
        }
        return TDataProviderBase::GetStatisticsProposalTransformer();
    }

    IGraphTransformer& GetLogicalOptProposalTransformer() override {
        if (auto* provider = Peek()) {
            return provider->GetLogicalOptProposalTransformer();
        }
        return TDataProviderBase::GetLogicalOptProposalTransformer();
    }

    IGraphTransformer& GetPhysicalOptProposalTransformer() override {
        if (auto* provider = Peek()) {
            return provider->GetPhysicalOptProposalTransformer();
        }
        return TDataProviderBase::GetPhysicalOptProposalTransformer();
    }

    IGraphTransformer& GetPhysicalFinalizingTransformer() override {
        if (auto* provider = Peek()) {
            return provider->GetPhysicalFinalizingTransformer();
        }
        return TDataProviderBase::GetPhysicalFinalizingTransformer();
    }

    IGraphTransformer& GetLoadTableMetadataTransformer() override {
        if (auto* provider = Peek()) {
            return provider->GetLoadTableMetadataTransformer();
        }
        return TDataProviderBase::GetLoadTableMetadataTransformer();
    }

    bool IsPersistent(const TExprNode& node) override {
        if (auto* provider = Peek()) {
            return provider->IsPersistent(node);
        }
        return TDataProviderBase::IsPersistent(node);
    }

    bool IsRead(const TExprNode& node) override {
        if (auto* provider = Peek()) {
            return provider->IsRead(node);
        }
        return TDataProviderBase::IsRead(node);
    }

    bool IsWrite(const TExprNode& node) override {
        if (auto* provider = Peek()) {
            return provider->IsWrite(node);
        }
        return TDataProviderBase::IsWrite(node);
    }

    bool CanBuildResult(const TExprNode& node, TSyncMap& syncList) override {
        if (auto* provider = Peek()) {
            return provider->CanBuildResult(node, syncList);
        }
        return TDataProviderBase::CanBuildResult(node, syncList);
    }

    bool CanPullResult(const TExprNode& node, TSyncMap& syncList, bool& canRef) override {
        if (auto* provider = Peek()) {
            return provider->CanPullResult(node, syncList, canRef);
        }
        return TDataProviderBase::CanPullResult(node, syncList, canRef);
    }

    bool GetExecWorld(const TExprNode::TPtr& node, TExprNode::TPtr& root) override {
        if (auto* provider = Peek()) {
            return provider->GetExecWorld(node, root);
        }
        return TDataProviderBase::GetExecWorld(node, root);
    }

    bool CanEvaluate(const TExprNode& node) override {
        if (auto* provider = Peek()) {
            return provider->CanEvaluate(node);
        }
        return TDataProviderBase::CanEvaluate(node);
    }

    void EnterEvaluation(ui64 id) override {
        RunOrDefer([id](IDataProvider& provider) { provider.EnterEvaluation(id); });
    }

    void LeaveEvaluation(ui64 id) override {
        RunOrDefer([id](IDataProvider& provider) { provider.LeaveEvaluation(id); });
    }

    TExprNode::TPtr CleanupWorld(const TExprNode::TPtr& node, TExprContext& ctx) override {
        if (auto* provider = Peek()) {
            return provider->CleanupWorld(node, ctx);
        }
        return TDataProviderBase::CleanupWorld(node, ctx);
    }

    TExprNode::TPtr OptimizePull(const TExprNode::TPtr& source, const TFillSettings& fillSettings, TExprContext& ctx, IOptimizationContext& optCtx) override {
        return Get().OptimizePull(source, fillSettings, ctx, optCtx);
    }

    void RegisterWorldArg(const TExprNode::TPtr& arg, const TExprNode::TPtr& world) override {
        RunOrDefer([arg, world](IDataProvider& provider) { provider.RegisterWorldArg(arg, world); });
    }

    bool CanExecute(const TExprNode& node) override {
        if (auto* provider = Peek()) {
            return provider->CanExecute(node);
        }
        return TDataProviderBase::CanExecute(node);
    }

    bool ValidateExecution(const TExprNode& node, TExprContext& ctx) override {
        return Get().ValidateExecution(node, ctx);
    }

    void GetRequiredChildren(const TExprNode& node, TExprNode::TListType& children) override {
        return Get().GetRequiredChildren(node, children);
    }

    IGraphTransformer& GetCallableExecutionTransformer() override {
        if (auto* provider = Peek()) {
            return provider->GetCallableExecutionTransformer();
        }
        return TDataProviderBase::GetCallableExecutionTransformer();
    }

    IGraphTransformer& GetFinalizingTransformer() override {
        if (auto* provider = Peek()) {
            return provider->GetFinalizingTransformer();
        }
        return TDataProviderBase::GetFinalizingTransformer();
    }

    bool CollectDiagnostics(NYson::TYsonWriter& writer) override {
        if (auto* provider = Peek()) {
            return provider->CollectDiagnostics(writer);
        }
        return TDataProviderBase::CollectDiagnostics(writer);
    }

    bool GetTasksInfo(NYson::TYsonWriter& writer) override {
        if (auto* provider = Peek()) {
            return provider->GetTasksInfo(writer);
        }
        return TDataProviderBase::GetTasksInfo(writer);
    }

    bool CollectStatistics(NYson::TYsonWriter& writer, bool totalOnly) override {
        if (auto* provider = Peek()) {
            return provider->CollectStatistics(writer, totalOnly);
        }
        return TDataProviderBase::CollectStatistics(writer, totalOnly);
    }

    bool CollectDiscoveredData(NYson::TYsonWriter& writer) override {
        if (auto* provider = Peek()) {
            return provider->CollectDiscoveredData(writer);
        }
        return TDataProviderBase::CollectDiscoveredData(writer);
    }

    IPlanFormatter& GetPlanFormatter() override {
        if (auto* provider = Peek()) {
            return provider->GetPlanFormatter();
        }
        return TDataProviderBase::GetPlanFormatter();
    }

    ITrackableNodeProcessor& GetTrackableNodeProcessor() override {
        if (auto* provider = Peek()) {
            return provider->GetTrackableNodeProcessor();
        }
        return TDataProviderBase::GetTrackableNodeProcessor();
    }

    IGraphTransformer& GetPlanInfoTransformer() override {
        if (auto* provider = Peek()) {
            return provider->GetPlanInfoTransformer();
        }
        return TDataProviderBase::GetPlanInfoTransformer();
    }

    IDqIntegration* GetDqIntegration() override {
        if (auto* provider = Peek()) {
            return provider->GetDqIntegration();
        }
        return TDataProviderBase::GetDqIntegration();
    }

    IDqOptimization* GetDqOptimization() override {
        if (auto* provider = Peek()) {
            return provider->GetDqOptimization();
        }
        return TDataProviderBase::GetDqOptimization();
    }

    IYtflowIntegration* GetYtflowIntegration() override {
        if (auto* provider = Peek()) {
            return provider->GetYtflowIntegration();
        }
        return TDataProviderBase::GetYtflowIntegration();
    }

    IYtflowOptimization* GetYtflowOptimization() override {
        if (auto* provider = Peek()) {
            return provider->GetYtflowOptimization();
        }
        return TDataProviderBase::GetYtflowOptimization();
    }

    bool IsFullCaptureReady() override {
        if (auto* provider = Peek()) {
            return provider->IsFullCaptureReady();
        }
        return TDataProviderBase::IsFullCaptureReady();
    }

private:
    TVector<std::function<void(IDataProvider&)>>& PendingActions() {
        return Source ? State->PendingSourceActions : State->PendingSinkActions;
    }

    void RunOrDefer(std::function<void(IDataProvider&)> action) {
        if (auto* provider = Peek()) {
            action(*provider);
        } else {
            PendingActions().push_back(std::move(action));
        }
    }

    IDataProvider* Peek() const {
        return Source ? State->Providers.Source.Get() : State->Providers.Sink.Get();
    }

    IDataProvider& Get() const {
        State->Ensure();
        return *Peek();
    }

    const TString Name;
    const TString CallablePrefix;
    const std::shared_ptr<TLazyProviderState> State;
    const bool Source;
    const NLayers::ILayersIntegrationPtr Layers;
};

} // namespace

TDataProviderInfo CreateLazyProvider(TStringBuf name, TStringBuf callablePrefix,
    std::function<TDataProviderInfo()> factory, bool hasLayers)
{
    auto state = std::make_shared<TLazyProviderState>(std::move(factory));
    TDataProviderInfo result;
    result.Source = MakeIntrusive<TLazyProvider>(name, callablePrefix, state, true, hasLayers);
    result.Sink = MakeIntrusive<TLazyProvider>(name, callablePrefix, std::move(state), false, false);
    return result;
}

TAutoPtr<IGraphTransformer> CreateLazyTransformer(std::function<TAutoPtr<IGraphTransformer>()> factory) {
    return new TLazyTransformer(std::move(factory));
}

} // namespace NKikimr::NKqp
