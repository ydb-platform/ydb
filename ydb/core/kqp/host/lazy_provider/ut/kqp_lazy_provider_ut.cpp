#include <ydb/core/kqp/host/lazy_provider/kqp_lazy_provider.h>

#include <yql/essentials/core/yql_opt_proposed_by_data.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {
namespace {
using namespace NYql;

struct TCalls {
    size_t Factories = 0;
    size_t Clusters = 0;
    size_t Validations = 0;
    size_t Rewinds = 0;
    TVector<ui64> Evaluations;
    TVector<ui64> Worlds;
    TVector<ui64>* EvaluationOrder = nullptr;
};

class TLayers final: public NLayers::ILayersIntegration {
public:
    TMaybe<NLayers::TLocations> ResolveLayers(const TVector<NLayers::TKey>&, const TString&, TExprContext&) const override {
        return {};
    }
    bool UpdateLayerCache(const NLayers::TKey&, const NLayers::TLayerInfo&, TExprContext&) override {
        ++Updates;
        return true;
    }
    bool UpdateLayerLocations(const NLayers::TKey&, NLayers::TLocations&&, TExprContext&) override {
        ++Updates;
        return true;
    }
    void RemoveLayerByName(const TString&) override { ++Removals; }

    size_t Updates = 0;
    size_t Removals = 0;
};

class TProvider final: public TDataProviderBase {
public:
    explicit TProvider(TCalls& calls, NLayers::ILayersIntegrationPtr layers = {})
        : Calls(calls)
        , Layers(std::move(layers))
    {
    }

    TStringBuf GetName() const override { return "test"; }

    bool ValidateParameters(TExprNode&, TExprContext&, TMaybe<TString>& cluster) override {
        ++Calls.Validations;
        cluster = "cluster";
        return true;
    }

    void AddCluster(const TString&, const THashMap<TString, TString>&) override { ++Calls.Clusters; }
    bool CanParse(const TExprNode& node) override { return node.IsCallable({"TestRead", "Auxiliary"}); }
    bool CanExecute(const TExprNode& node) override { return CanParse(node); }
    bool IsFullCaptureReady() override { return true; }
    NLayers::ILayersIntegrationPtr GetLayersIntegration() const override { return Layers; }
    void Reset() override { ++Calls.Rewinds; }
    void EnterEvaluation(ui64 id) override {
        Calls.Evaluations.push_back(id);
        if (Calls.EvaluationOrder) {
            Calls.EvaluationOrder->push_back(id);
        }
    }
    void LeaveEvaluation(ui64 id) override {
        Calls.Evaluations.push_back(id + 100);
        if (Calls.EvaluationOrder) {
            Calls.EvaluationOrder->push_back(id + 100);
        }
    }
    void RegisterWorldArg(const TExprNode::TPtr& arg, const TExprNode::TPtr&) override { Calls.Worlds.push_back(arg->UniqueId()); }

private:
    TCalls& Calls;
    NLayers::ILayersIntegrationPtr Layers;
};

TDataProviderInfo MakeLazy(TCalls& source, TCalls& sink) {
    return CreateLazyProvider("test", "Test", [&] {
        ++source.Factories;
        ++sink.Factories;
        TDataProviderInfo providers;
        providers.Source = MakeIntrusive<TProvider>(source);
        providers.Sink = MakeIntrusive<TProvider>(sink);
        return providers;
    });
}

} // namespace

Y_UNIT_TEST_SUITE(KqpLazyProvider) {
    Y_UNIT_TEST(PipelineSelectionWaitsForMetadataAndPreservesAsyncAndRewind) {
        class TAsyncTransformer final: public TGraphTransformerBase {
        public:
            explicit TAsyncTransformer(size_t& rewinds) : Rewinds(rewinds) {}
        private:
            TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext&) override {
                output = input;
                return TStatus::Async;
            }
            NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode&) override {
                return NThreading::MakeFuture();
            }
            TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext&) override {
                output = input;
                return TStatus::Ok;
            }
            void Rewind() override { ++Rewinds; }
            size_t& Rewinds;
        };
        bool metadataReady = false;
        size_t factories = 0, rewinds = 0;
        auto transformer = CreateLazyTransformer([&]() -> TAutoPtr<IGraphTransformer> {
            UNIT_ASSERT(metadataReady);
            ++factories;
            return new TAsyncTransformer(rewinds);
        });
        transformer->Rewind();
        UNIT_ASSERT_VALUES_EQUAL(factories, 0);
        metadataReady = true;
        TExprContext ctx;
        auto input = ctx.NewWorld(TPositionHandle());
        TExprNode::TPtr output;
        for (size_t i = 0; i < 2; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(transformer->Transform(input, output, ctx).Level, IGraphTransformer::TStatus::Async);
            transformer->GetAsyncFuture(*input).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(transformer->ApplyAsyncChanges(input, output, ctx).Level, IGraphTransformer::TStatus::Ok);
            UNIT_ASSERT_VALUES_EQUAL(output, input);
            transformer->Rewind();
        }
        UNIT_ASSERT_VALUES_EQUAL(factories, 1);
        UNIT_ASSERT_VALUES_EQUAL(rewinds, 2);
    }

    Y_UNIT_TEST(LayerRegistrationIsLazyButEarlyUpdatesArePreserved) {
        TCalls source, sink;
        auto layers = MakeIntrusive<TLayers>();
        auto providers = CreateLazyProvider("test", "Test", [&] {
            ++source.Factories;
            TDataProviderInfo providers;
            providers.Source = MakeIntrusive<TProvider>(source, layers);
            providers.Sink = MakeIntrusive<TProvider>(sink);
            return providers;
        }, true);
        auto integration = providers.Source->GetLayersIntegration();
        UNIT_ASSERT(integration);
        UNIT_ASSERT_VALUES_EQUAL(source.Factories, 0);
        TExprContext ctx;
        UNIT_ASSERT(integration->UpdateLayerCache(NLayers::TKey("base"), {}, ctx));
        UNIT_ASSERT_VALUES_EQUAL(source.Factories, 1);
        UNIT_ASSERT_VALUES_EQUAL(layers->Updates, 1);
        integration->RemoveLayerByName("base");
        UNIT_ASSERT_VALUES_EQUAL(layers->Removals, 1);
    }

    Y_UNIT_TEST(UnusedProviderSurvivesAllCompilerPhasesWithoutConstruction) {
        TCalls source, sink;
        auto providers = MakeLazy(source, sink);
        TExprContext ctx;
        auto input = ctx.NewWorld(TPositionHandle());
        TExprNode::TPtr output;
        TTypeAnnotationContext types;
        types.AddDataSource("test", providers.Source);
        types.AddDataSink("test", providers.Sink);
        for (auto* provider : {providers.Source.Get(), providers.Sink.Get()}) {
            UNIT_ASSERT(provider->Initialize(ctx));
            UNIT_ASSERT_VALUES_EQUAL(provider->GetName(), "test");
            UNIT_ASSERT(!provider->CanParse(*input));
            UNIT_ASSERT(!provider->GetDqIntegration());
            UNIT_ASSERT(!provider->GetLayersIntegration());
            provider->GetConfigurationTransformer().Rewind();
            provider->GetIntentDeterminationTransformer().Rewind();
            provider->GetLoadTableMetadataTransformer().Rewind();
            provider->GetFinalizingTransformer().Transform(input, output, ctx);
            provider->Reset();
        }
        auto logical = CreateLogicalDataProposalsInspector(types);
        auto physical = CreatePhysicalDataProposalsInspector(types);
        for (auto* transformer : {logical.Get(), physical.Get()}) {
            transformer->Rewind();
            UNIT_ASSERT_VALUES_EQUAL(transformer->Transform(input, output, ctx).Level, IGraphTransformer::TStatus::Ok);
            transformer->Rewind();
        }
        UNIT_ASSERT_VALUES_EQUAL(source.Factories, 0);
        UNIT_ASSERT_VALUES_EQUAL(sink.Factories, 0);
    }

    Y_UNIT_TEST(MetadataActivatesSourceAndSinkExactlyOnce) {
        TCalls source, sink;
        auto providers = MakeLazy(source, sink);
        providers.Source->AddCluster("external", {});
        providers.Source->AddCluster("another", {});
        providers.Sink->AddCluster("external", {});
        UNIT_ASSERT_VALUES_EQUAL(source.Factories, 1);
        UNIT_ASSERT_VALUES_EQUAL(sink.Factories, 1);
        UNIT_ASSERT_VALUES_EQUAL(source.Clusters, 2);
        UNIT_ASSERT_VALUES_EQUAL(sink.Clusters, 1);
        UNIT_ASSERT(providers.Sink->IsFullCaptureReady());
    }

    Y_UNIT_TEST(ProviderReferenceOrPragmaActivatesBothEndpoints) {
        TCalls source, sink;
        auto providers = MakeLazy(source, sink);
        TExprContext ctx;
        auto node = ctx.NewCallable(TPositionHandle(), "DataSource", {ctx.NewAtom(TPositionHandle(), "test")});
        TMaybe<TString> cluster;
        UNIT_ASSERT(providers.Source->ValidateParameters(*node, ctx, cluster));
        UNIT_ASSERT_VALUES_EQUAL(cluster, "cluster");
        UNIT_ASSERT_VALUES_EQUAL(source.Validations, 1);
        UNIT_ASSERT_VALUES_EQUAL(sink.Factories, 1);
    }

    Y_UNIT_TEST(RawProviderCallableActivatesWithoutReadOrWrite) {
        TCalls source, sink;
        auto providers = MakeLazy(source, sink);
        TExprContext ctx;
        auto unrelated = ctx.NewCallable(TPositionHandle(), "Testing", {});
        UNIT_ASSERT(!providers.Source->CanParse(*unrelated));
        UNIT_ASSERT_VALUES_EQUAL(source.Factories, 0);
        auto node = ctx.NewCallable(TPositionHandle(), "TestRead", {});
        UNIT_ASSERT(providers.Source->CanParse(*node));
        UNIT_ASSERT(providers.Sink->CanExecute(*node));
        UNIT_ASSERT_VALUES_EQUAL(source.Factories, 1);
    }

    Y_UNIT_TEST(ActivationDuringEvaluationReplaysContext) {
        TCalls source, sink;
        auto providers = MakeLazy(source, sink);
        TExprContext ctx;
        auto world = ctx.NewWorld(TPositionHandle());
        providers.Source->EnterEvaluation(7);
        providers.Sink->RegisterWorldArg(world, world);
        UNIT_ASSERT_VALUES_EQUAL(source.Factories, 0);
        providers.Source->AddCluster("external", {});
        providers.Source->LeaveEvaluation(7);
        UNIT_ASSERT_VALUES_EQUAL(source.Evaluations, (TVector<ui64>{7, 107}));
        UNIT_ASSERT_VALUES_EQUAL(sink.Worlds, (TVector<ui64>{world->UniqueId()}));
        providers.Sink->Reset();
        UNIT_ASSERT_VALUES_EQUAL(sink.Rewinds, 1);
    }

    Y_UNIT_TEST(DeferredCallbacksPreserveOrderAcrossSourceAndSink) {
        TCalls source, sink;
        TVector<ui64> order;
        source.EvaluationOrder = &order;
        sink.EvaluationOrder = &order;
        auto providers = MakeLazy(source, sink);
        providers.Source->EnterEvaluation(1);
        providers.Sink->EnterEvaluation(2);
        providers.Source->LeaveEvaluation(1);
        providers.Sink->LeaveEvaluation(2);
        UNIT_ASSERT(order.empty());
        providers.Source->AddCluster("external", {});
        UNIT_ASSERT_VALUES_EQUAL(order, (TVector<ui64>{1, 2, 101, 102}));
    }

    Y_UNIT_TEST(NonPrefixedProviderHelpersActivateBeforeAnyDataSource) {
        TCalls source, sink;
        auto providers = CreateLazyProvider("test", "Test", [&] {
            ++source.Factories;
            TDataProviderInfo providers;
            providers.Source = MakeIntrusive<TProvider>(source);
            providers.Sink = MakeIntrusive<TProvider>(sink);
            return providers;
        }, false, {"Auxiliary"});
        TExprContext ctx;
        auto unrelated = ctx.NewCallable(TPositionHandle(), "Unrelated", {});
        UNIT_ASSERT(!providers.Source->CanParse(*unrelated));
        UNIT_ASSERT_VALUES_EQUAL(source.Factories, 0);
        auto auxiliary = ctx.NewCallable(TPositionHandle(), "Auxiliary", {});
        UNIT_ASSERT(providers.Sink->CanParse(*auxiliary));
        UNIT_ASSERT(providers.Source->CanExecute(*auxiliary));
        UNIT_ASSERT_VALUES_EQUAL(source.Factories, 1);
    }

    Y_UNIT_TEST(FailedInitializationIsNotRetried) {
        size_t attempts = 0;
        auto providers = CreateLazyProvider("test", "Test", [&]() -> TDataProviderInfo {
            ++attempts;
            ythrow yexception() << "provider initialization failed";
        });
        UNIT_ASSERT_EXCEPTION_CONTAINS(providers.Source->AddCluster("external", {}), yexception, "initialization failed");
        UNIT_ASSERT_EXCEPTION_CONTAINS(providers.Sink->AddCluster("external", {}), yexception, "initialization failed");
        UNIT_ASSERT_VALUES_EQUAL(attempts, 1);
    }
}

} // namespace NKikimr::NKqp
