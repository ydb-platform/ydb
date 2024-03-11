#include "testactorsys.h"
#include <ydb/core/tablet/bootstrapper.h>
#include <ydb/core/tx/scheme_board/replica.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/time_provider/time_provider.h>
#include <ydb/core/control/immediate_control_board_impl.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/nameservice.h>
#include <ydb/core/base/channel_profiles.h>
#include <ydb/core/base/domain.h>

#include <util/generic/singleton.h>

namespace NKikimr {

class TActorNameTracker {
public:
    static TActorNameTracker& GetInstance() {
        auto* instance = Singleton<TActorNameTracker>();
        return *instance;
    }

    void Register(const TActorId& actorId, const TString& name) {
        TGuard<TMutex> guard{Mutex};
        NameByActorIdString[actorId] = name + actorId.ToString();
    }

    TString GetName(const TActorId& actorId) const {
        TGuard<TMutex> guard{Mutex};
        auto it = NameByActorIdString.find(actorId);
        if (it == NameByActorIdString.end()) {
            return "[unknown_actor]" + actorId.ToString();
        }

        return it->second;
    }

private:
    THashMap<TActorId, TString> NameByActorIdString;
    TMutex Mutex;

};

void RegisterActorName(const TActorId& actorId, const TString& name) {
    TActorNameTracker::GetInstance().Register(actorId, name);
}

TString GetRegisteredActorName(const TActorId& actorId) {
    return TActorNameTracker::GetInstance().GetName(actorId);
}

class TTestExecutorPool : public IExecutorPool {
    TTestActorSystem *Context;
    const ui32 NodeId;

public:
    TTestExecutorPool(TTestActorSystem *context, ui32 nodeId)
        : IExecutorPool(0)
        , Context(context)
        , NodeId(nodeId)
    {}

    ui32 GetReadyActivation(TWorkerContext& /*wctx*/, ui64 /*revolvingCounter*/) override {
        Y_ABORT();
    }

    void ReclaimMailbox(TMailboxType::EType /*mailboxType*/, ui32 /*hint*/, NActors::TWorkerId /*workerId*/, ui64 /*revolvingCounter*/) override {
        Y_ABORT();
    }

    TMailboxHeader *ResolveMailbox(ui32 hint) override {
        const auto it = Context->Mailboxes.find({NodeId, PoolId, hint});
        return it != Context->Mailboxes.end() ? &it->second.Header : nullptr;
    }

    void Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, NActors::TWorkerId /*workerId*/) override {
        Context->Schedule(deadline, ev, cookie, NodeId);
    }

    void Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, NActors::TWorkerId /*workerId*/) override {
        Context->Schedule(TInstant::FromValue(deadline.GetValue()), ev, cookie, NodeId);
    }

    void Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, NActors::TWorkerId /*workerId*/) override {
        Context->Schedule(delta, ev, cookie, NodeId);
    }

    bool Send(TAutoPtr<IEventHandle>& ev) override {
        if (TlsActivationContext) {
            const TActorContext& ctx = TActivationContext::AsActorContext();
            IActor* sender = Context->GetActor(ctx.SelfID);
            TTestDecorator* decorator = dynamic_cast<TTestDecorator*>(sender);
            if (decorator && !decorator->BeforeSending(ev)) {
                ev = nullptr;
            }
        }
        return Context->Send(ev, NodeId);
    }

    bool SpecificSend(TAutoPtr<IEventHandle>& ev) override {
        return Send(ev);
    }

    void ScheduleActivation(ui32 /*activation*/) override {
        Y_ABORT();
    }

    void SpecificScheduleActivation(ui32 /*activation*/) override {
        Y_ABORT();
    }

    void ScheduleActivationEx(ui32 /*activation*/, ui64 /*revolvingCounter*/) override {
        Y_ABORT();
    }

    TActorId Register(IActor* actor, TMailboxType::EType /*mailboxType*/, ui64 /*revolvingCounter*/, const TActorId& parentId) override {
        return Context->Register(actor, parentId, PoolId, std::nullopt, NodeId);
    }

    TActorId Register(IActor* actor, TMailboxHeader* /*mailbox*/, ui32 hint, const TActorId& parentId) override {
        return Context->Register(actor, parentId, PoolId, hint, NodeId);
    }

    void Prepare(TActorSystem* /*actorSystem*/, NSchedulerQueue::TReader** /*scheduleReaders*/, ui32* /*scheduleSz*/) override {
    }

    void Start() override {
    }

    void PrepareStop() override {
    }

    void Shutdown() override {
    }

    bool Cleanup() override {
        return true;
    }

    TAffinity* Affinity() const override {
        Y_ABORT();
    }
};

static TActorId MakeBoardReplicaID(ui32 node, ui32 replicaIndex) {
    char x[12] = {'s', 's', 'b'};
    x[3] = (char)1;
    memcpy(x + 5, &replicaIndex, sizeof(ui32));
    return TActorId(node, TStringBuf(x, 12));
}

NTabletPipe::TClientConfig TTestActorSystem::GetPipeConfigWithRetries() {
    NTabletPipe::TClientConfig pipeConfig;
    pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
    return pipeConfig;
}

void TTestActorSystem::SendToPipe(ui64 tabletId, const TActorId& sender, IEventBase* payload, ui64 cookie, const NKikimr::NTabletPipe::TClientConfig& pipeConfig) {
    WrapInActorContext(sender, [&] { // perform action in sender's context
        const TActorId clientId = Register(NKikimr::NTabletPipe::CreateClient(sender, tabletId, pipeConfig));
        NTabletPipe::SendData(sender, clientId, payload, cookie);
        Send(new IEventHandle(clientId, sender, new NKikimr::TEvTabletPipe::TEvShutdown()));
    });
}

TTabletStorageInfo *TTestActorSystem::CreateTestTabletInfo(ui64 tabletId, TTabletTypes::EType tabletType,
        TBlobStorageGroupType::EErasureSpecies erasure, ui32 groupId, ui32 numChannels) {
    auto x = std::make_unique<TTabletStorageInfo>();

    x->TabletID = tabletId;
    x->TabletType = tabletType;
    x->Channels.resize(numChannels);

    for (ui64 channel = 0; channel < x->Channels.size(); ++channel) {
        x->Channels[channel].Channel = channel;
        x->Channels[channel].Type = TBlobStorageGroupType(erasure);
        x->Channels[channel].History.resize(1);
        x->Channels[channel].History[0].FromGeneration = 0;
        x->Channels[channel].History[0].GroupID = groupId;
    }

    return x.release();
}

TActorId TTestActorSystem::CreateTestBootstrapper(TTabletStorageInfo *info, std::function<IActor*(TActorId, TTabletStorageInfo*)> op, ui32 nodeId) {
    auto bi = MakeIntrusive<TBootstrapperInfo>(new TTabletSetupInfo(op, TMailboxType::Simple, 0, TMailboxType::Simple, 0));
    return Register(CreateBootstrapper(info, bi.Get()), nodeId);
}

void TTestActorSystem::SetupTabletRuntime(ui32 numDataCenters, ui32 stateStorageNodeId, ui32 targetNodeId) {
    const ui32 nodeCountInDC = (MaxNodeId + numDataCenters - 1) / numDataCenters;
    auto locationGenerator = [&](ui32 nodeId) {
        const ui32 dcNum = (nodeId + nodeCountInDC - 1) / nodeCountInDC;
        NActorsInterconnect::TNodeLocation location;
        location.SetDataCenter(ToString(dcNum));
        location.SetRack(ToString(nodeId));
        location.SetUnit(ToString(nodeId));
        return TNodeLocation(location);
    };
    SetupTabletRuntime(locationGenerator, stateStorageNodeId, targetNodeId);
}

void TTestActorSystem::SetupTabletRuntime(const std::function<TNodeLocation(ui32)>& locationGenerator,
        ui32 stateStorageNodeId, ui32 targetNodeId) {
    auto setup = MakeIntrusive<TTableNameserverSetup>();
    for (ui32 nodeId : GetNodes()) {
        const TString name = Sprintf("127.0.0.%u", nodeId);
        setup->StaticNodeTable[nodeId] = {name, name, name, 19001, locationGenerator(nodeId)};
    }

    for (ui32 nodeId : GetNodes()) {
        if (!stateStorageNodeId) {
            stateStorageNodeId = nodeId;
        }
        if (targetNodeId == 0 || targetNodeId == nodeId) {
            SetupStateStorage(nodeId, stateStorageNodeId);
            SetupTabletResolver(nodeId);
            RegisterService(GetNameserviceActorId(), Register(CreateNameserverTable(setup), nodeId));
        }
    }
}

void TTestActorSystem::SetupStateStorage(ui32 nodeId, ui32 stateStorageNodeId) {
    if (const auto& domain = GetDomainsInfo()->Domain) {
        ui32 numReplicas = 3;

        auto process = [&](auto&& generateId, auto&& createReplica) {
            auto info = MakeIntrusive<TStateStorageInfo>();
            info->NToSelect = numReplicas;
            info->Rings.resize(numReplicas);
            for (ui32 i = 0; i < numReplicas; ++i) {
                info->Rings[i].Replicas.push_back(generateId(stateStorageNodeId, i));
            }
            if (nodeId == stateStorageNodeId) {
                for (ui32 i = 0; i < numReplicas; ++i) {
                    RegisterService(generateId(stateStorageNodeId, i), Register(createReplica(info.Get(), i), nodeId));
                }
            }
            return info;
        };

        auto ss = process(MakeStateStorageReplicaID, CreateStateStorageReplica);
        auto b = process(MakeBoardReplicaID, CreateStateStorageBoardReplica);
        auto sb = process(MakeSchemeBoardReplicaID, CreateSchemeBoardReplica);

        RegisterService(MakeStateStorageProxyID(), Register(CreateStateStorageProxy(ss.Get(), b.Get(), sb.Get()), nodeId));
    }
}

void TTestActorSystem::SetupTabletResolver(ui32 nodeId) {
    RegisterService(MakeTabletResolverID(),
        Register(CreateTabletResolver(MakeIntrusive<TTabletResolverConfig>()), nodeId));
}

IExecutorPool *TTestActorSystem::CreateTestExecutorPool(ui32 nodeId) {
    return new TTestExecutorPool(this, nodeId);
}

thread_local TTestActorSystem *TTestActorSystem::CurrentTestActorSystem = nullptr;

TIntrusivePtr<ITimeProvider> TTestActorSystem::CreateTimeProvider() {
    class TTestActorTimeProvider : public ITimeProvider {
    public:
        TInstant Now() override { return CurrentTestActorSystem->Clock; }
    };
    return MakeIntrusive<TTestActorTimeProvider>();
}

TIntrusivePtr<IMonotonicTimeProvider> TTestActorSystem::CreateMonotonicTimeProvider() {
    class TTestActorMonotonicTimeProvider : public IMonotonicTimeProvider {
    public:
        TMonotonic Now() override { return TMonotonic::MicroSeconds(CurrentTestActorSystem->Clock.MicroSeconds()); }
    };
    return MakeIntrusive<TTestActorMonotonicTimeProvider>();
}

const ui32 TTestActorSystem::SYSTEM_POOL_ID = 0;

}
