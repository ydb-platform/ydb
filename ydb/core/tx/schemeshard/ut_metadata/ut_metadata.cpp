#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/tiering/rule/object.h>

#include <ydb/services/metadata/container/snapshot.h>
#include <ydb/services/metadata/ds_table/accessor_snapshot_simple.h>
#include <ydb/services/metadata/service.h>

using namespace NKikimr::NSchemeShard;
using namespace NKikimr;
using namespace NKikimrSchemeOp;
using namespace NSchemeShardUT_Private;

static const TString defaultTieringRuleConfig = R"({
    "rules" : [
        {
            "tierName" : "tier1",
            "durationForEvict" : "10d"
        }
    ]
})";

static const TString defaultTierConfig = R"(
    Name: tier1
    ObjectStorage : {
        Endpoint: "fake"
        Bucket: "fake"
        SecretableAccessKey: {
            Value: {
                Data: "secretAccessKey"
            }
        }
        SecretableSecretKey: {
            Value: {
                Data: "secretSecretKey"
            }
        }
    }
)";

class IMetadataController {
private:
    void HandleDefault(const TActorContext&) const {
        Y_ABORT_S("Not implemented");
    }

public:
    using TPtr = std::shared_ptr<IMetadataController>;

    virtual void OnRefreshSubscriberData(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr /*ev*/, const TActorContext& ctx) {
        HandleDefault(ctx);
    }
    virtual void OnAskSnapshot(NMetadata::NProvider::TEvAskSnapshot::TPtr /*ev*/, const TActorContext& ctx) {
        HandleDefault(ctx);
    }
    virtual void OnAskExtendedSnapshot(NMetadata::NProvider::TEvAskExtendedSnapshot::TPtr /*ev*/, const TActorContext& ctx) {
        HandleDefault(ctx);
    }
    virtual void OnPrepareManager(NMetadata::NProvider::TEvPrepareManager::TPtr /*ev*/, const TActorContext& ctx) {
        HandleDefault(ctx);
    }
    virtual void OnSubscribe(NMetadata::NProvider::TEvSubscribeExternal::TPtr /*ev*/, const TActorContext& ctx) {
        HandleDefault(ctx);
    }
    virtual void OnUnsubscribe(NMetadata::NProvider::TEvUnsubscribeExternal::TPtr /*ev*/, const TActorContext& ctx) {
        HandleDefault(ctx);
    }
    virtual void OnObjectOperation(NMetadata::NProvider::TEvObjectsOperation::TPtr /*ev*/, const TActorContext& ctx) {
        HandleDefault(ctx);
    }

    virtual ~IMetadataController() = default;
};

class TMetadataMock: public NActors::TActor<TMetadataMock> {
private:
    IMetadataController::TPtr Controller;

private:
    using TBase = NActors::TActor<TMetadataMock>;

    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev, const TActorContext& ctx) {
        Controller->OnRefreshSubscriberData(ev, ctx);
    }

    void Handle(NMetadata::NProvider::TEvAskSnapshot::TPtr& ev, const TActorContext& ctx) {
        Controller->OnAskSnapshot(ev, ctx);
    }

    void Handle(NMetadata::NProvider::TEvAskExtendedSnapshot::TPtr& ev, const TActorContext& ctx) {
        Controller->OnAskExtendedSnapshot(ev, ctx);
    }

    void Handle(NMetadata::NProvider::TEvPrepareManager::TPtr& ev, const TActorContext& ctx) {
        Controller->OnPrepareManager(ev, ctx);
    }

    void Handle(NMetadata::NProvider::TEvSubscribeExternal::TPtr& ev, const TActorContext& ctx) {
        Controller->OnSubscribe(ev, ctx);
    }

    void Handle(NMetadata::NProvider::TEvUnsubscribeExternal::TPtr& ev, const TActorContext& ctx) {
        Controller->OnUnsubscribe(ev, ctx);
    }

    void Handle(NMetadata::NProvider::TEvObjectsOperation::TPtr& ev, const TActorContext& ctx) {
        Controller->OnObjectOperation(ev, ctx);
    }

public:
    STFUNC(StateMain) {
        Cerr << "MetadataMock: Handle " << ev->GetTypeName() << Endl;
        switch (ev->GetTypeRewrite()) {
            HFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle);
            HFunc(NMetadata::NProvider::TEvAskSnapshot, Handle);
            HFunc(NMetadata::NProvider::TEvAskExtendedSnapshot, Handle);
            HFunc(NMetadata::NProvider::TEvPrepareManager, Handle);
            HFunc(NMetadata::NProvider::TEvSubscribeExternal, Handle);
            HFunc(NMetadata::NProvider::TEvUnsubscribeExternal, Handle);
            HFunc(NMetadata::NProvider::TEvObjectsOperation, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
            default:
                AFL_VERIFY(false);
        }
    }

    TMetadataMock(IMetadataController::TPtr controller)
        : TBase(&TMetadataMock::StateMain)
        , Controller(controller) {
    }
};

class TSimpleMetadataController: public IMetadataController {
private:
    YDB_ACCESSOR_DEF(NMetadata::NContainer::TObjectContainer<NColumnShard::NTiers::TTieringRule>, TieringRules);

public:
    struct IResponse {
        using TPtr = std::unique_ptr<IResponse>;
        virtual void Send() = 0;
        virtual ~IResponse() = default;
    };

    template <typename F>
    class TResponse: public IResponse {
    private:
        F Func;

    public:
        TResponse(F func)
            : Func(std::move(func)) {
        }
        void Send() override {
            Func();
        }
    };

    template <typename F>
    static IResponse::TPtr MakeResponse(F replyFunc) {
        return std::make_unique<TResponse<F>>(std::move(replyFunc));
    }

protected:
    virtual void Reply(IResponse::TPtr response, const TString& /*tag*/) {
        response->Send();
    }

public:
    using TPtr = std::shared_ptr<IMetadataController>;

    void OnAskExtendedSnapshot(NMetadata::NProvider::TEvAskExtendedSnapshot::TPtr ev, const TActorContext& ctx) override {
        {
            auto managers = ev->Get()->GetFetcher()->GetManagers();
            AFL_VERIFY(managers.size() == 1);
            AFL_VERIFY(managers[0]->GetTypeId() == "TIERING_RULE");
        }
        auto snapshot = std::make_shared<NMetadata::NContainer::TSnapshot<NColumnShard::NTiers::TTieringRule>>(TieringRules, TInstant::Now());
        Reply(MakeResponse([ctx, sender = ev->Sender,
                               event = std::make_unique<NMetadata::NProvider::TDSAccessorSimple::TEvController::TEvResult>(snapshot)]() mutable {
            ctx.Send(sender, event.release());
        }),
            "EvAskExtendedSnapshot");
    }

    void OnObjectOperation(NMetadata::NProvider::TEvObjectsOperation::TPtr ev, const TActorContext& /*ctx*/) override {
        AFL_VERIFY(ev->Get()->GetCommand()->GetBehaviour()->GetTypeId() == "TIERING_RULE");
        Reply(MakeResponse([controller = ev->Get()->GetCommand()->GetController(), historyInstant = TInstant::Now()]() mutable {
            controller->OnAlteringFinished(historyInstant);
        }),
            "EvObjectsOperation");
    }
};

class TDelayingMetadataController: public TSimpleMetadataController {
private:
    using TBase = TSimpleMetadataController;
    THashMap<TString, NThreading::TPromise<IResponse::TPtr>> AwaitedResponses;

private:
    void Reply(IResponse::TPtr response, const TString& tag) override {
        auto findPromise = AwaitedResponses.find(tag);
        if (!findPromise.IsEnd()) {
            findPromise->second.SetValue(std::move(response));
            AwaitedResponses.erase(findPromise);
        } else {
            TBase::Reply(std::move(response), tag);
        }
    }

public:
    NThreading::TFuture<IResponse::TPtr> CatchResponse(const TString& tag) {
        auto emplaced = AwaitedResponses.emplace(tag, NThreading::NewPromise<IResponse::TPtr>());
        return emplaced.first->second.GetFuture();
    }
};

IActor* CreateMetadataServiceMock(IMetadataController::TPtr controller) {
    return new TMetadataMock(controller);
}

void SetupMetadataServiceMock(IMetadataController::TPtr controller, TTestActorRuntime& runtime, ui64 nodeIndex = 0) {
    TActorId serviceActor = runtime.Register(CreateMetadataServiceMock(controller), nodeIndex);
    runtime.RegisterService(NMetadata::NProvider::MakeServiceId(runtime.GetFirstNodeId() + nodeIndex), serviceActor, nodeIndex);
}

Y_UNIT_TEST_SUITE(TMetadata) {
    Y_UNIT_TEST(Basic) {
        auto controller = std::make_shared<TSimpleMetadataController>();

        TTestBasicRuntime runtime;
        TTestEnvOptions options;
        options.EnableTieringInColumnShard(true);
        TTestEnv env(runtime, options);
        SetupMetadataServiceMock(controller, runtime);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::METADATA_PROVIDER, NActors::NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::METADATA_INITIALIZER, NActors::NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::METADATA_MANAGER, NActors::NLog::PRI_DEBUG);

        env.TestWaitMetadataInitialization(runtime, "TIERING_RULE");

        NKikimrSchemeOp::TModifyObjectDescription request;
        request.MutableContext()->SetActivityType(
            ::NKikimrSchemeOp::TObjectModificationContext_EActivityType::TObjectModificationContext_EActivityType_Create);
        {
            auto* settings = request.MutableSettings();
            settings->SetType("TIERING_RULE");
            settings->SetObject("tiering1");
            settings->SetExistingOk(false);
            settings->SetMissingOk(true);
            settings->SetReplaceIfExists(false);
            {
                ::NKikimrSchemeOp::TModifyObjectSettings_TFeatureValue defaultColumn;
                defaultColumn.SetStringValue("timestamp");
                settings->MutableFeatures()->emplace("defaultColumn", std::move(defaultColumn));
            }
            {
                ::NKikimrSchemeOp::TModifyObjectSettings_TFeatureValue description;
                description.SetStringValue(defaultTieringRuleConfig);
                settings->MutableFeatures()->emplace("description", std::move(description));
            }
        }

        TActorId sender = runtime.AllocateEdgeActor();
        auto evModifyObject = new TEvSchemeShard::TEvModifyObject(request);

        ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, evModifyObject);

        TAutoPtr<IEventHandle> handle;
        auto event = runtime.GrabEdgeEvent<TEvSchemeShard::TEvModifyObjectResult>(handle);
        UNIT_ASSERT_C(!event->Record.HasError(), event->Record.GetError());
    }

    void TestRebootUnderModification(bool isModificationSuccessful) {
        auto controller = std::make_shared<TDelayingMetadataController>();

        TTestBasicRuntime runtime;
        TTestEnvOptions options;
        options.EnableTieringInColumnShard(true);
        TTestEnv env(runtime, options);
        SetupMetadataServiceMock(controller, runtime);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::METADATA_PROVIDER, NActors::NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::METADATA_INITIALIZER, NActors::NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::METADATA_MANAGER, NActors::NLog::PRI_DEBUG);

        env.TestWaitMetadataInitialization(runtime, "TIERING_RULE");

        auto responseFuture = controller->CatchResponse("EvObjectsOperation");

        NKikimrSchemeOp::TModifyObjectDescription request;
        request.MutableContext()->SetActivityType(
            ::NKikimrSchemeOp::TObjectModificationContext_EActivityType::TObjectModificationContext_EActivityType_Create);
        {
            auto* settings = request.MutableSettings();
            settings->SetType("TIERING_RULE");
            settings->SetObject("tiering1");
            settings->SetExistingOk(false);
            settings->SetMissingOk(true);
            settings->SetReplaceIfExists(false);
            {
                ::NKikimrSchemeOp::TModifyObjectSettings_TFeatureValue defaultColumn;
                defaultColumn.SetStringValue("timestamp");
                settings->MutableFeatures()->emplace("defaultColumn", std::move(defaultColumn));
            }
            {
                ::NKikimrSchemeOp::TModifyObjectSettings_TFeatureValue description;
                description.SetStringValue(defaultTieringRuleConfig);
                settings->MutableFeatures()->emplace("description", std::move(description));
            }
        }

        TActorId sender = runtime.AllocateEdgeActor();
        auto evModifyObject = new TEvSchemeShard::TEvModifyObject(request);

        ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, evModifyObject);

        runtime.WaitFuture(responseFuture, TDuration::Seconds(10));

        if (isModificationSuccessful) {
            NColumnShard::NTiers::TTieringRule tiering;
            tiering.SetTieringRuleId("tiering1");
            tiering.SetDefaultColumn("timestamp");
            tiering.SetIntervals({ NColumnShard::NTiers::TTieringInterval("tier1", TDuration::Days(10)) });
            NMetadata::NContainer::TObjectContainer<NColumnShard::NTiers::TTieringRule> tieringRules;
            tieringRules.Emplace("tiering1", std::make_shared<NMetadata::NContainer::TObjectSnapshot<NColumnShard::NTiers::TTieringRule>>(
                                                 tiering, TInstant::Now() /*TODO: wring timestamp*/));
            controller->SetTieringRules(tieringRules);
        }

        RebootTablet(runtime, TTestTxConfig::SchemeShard, TActorId());
        env.TestWaitMetadataInitialization(runtime, "TIERING_RULE");

        TAutoPtr<IEventHandle> handle;
        auto event = runtime.GrabEdgeEvent<TEvSchemeShard::TEvModifyObjectResult>(handle);
        if (isModificationSuccessful) {
            UNIT_ASSERT_C(!event->Record.HasError(), event->Record.GetError());
        } else {
            UNIT_ASSERT(event->Record.HasError());
        }
    }

    Y_UNIT_TEST(RebootUnderSuccessfulModification) {
        TestRebootUnderModification(true);
    }

    Y_UNIT_TEST(RebootUnderFaultyModification) {
        TestRebootUnderModification(false);
    }
}
