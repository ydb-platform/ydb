#pragma once

#include "defs.h"
#include "events.h"
#include "events_internal.h"
#include "subscriber.h"

#include <ydb/core/base/tablet_types.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/fake_coordinator.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_allocator/txallocator.h>

#include <ydb/library/actors/interconnect/interconnect_impl.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>

namespace NKikimr {
namespace NSchemeBoard {

class TTestContext: public TTestBasicRuntime {
public:
    using TTestBasicRuntime::TTestBasicRuntime;

    void Send(
        const TActorId& recipient,
        const TActorId& sender,
        IEventBase* ev,
        ui32 flags = 0,
        ui64 cookie = 0,
        ui32 senderNodeIndex = 0,
        bool viaActorSystem = false
    ) {
        TTestBasicRuntime::Send(new IEventHandle(recipient, sender, ev, flags, cookie), senderNodeIndex, viaActorSystem);
    }

    void WaitForEvent(ui32 eventType) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(eventType);
        DispatchEvents(options);
    }

    void Connect(ui32 nodeIndexFrom, ui32 nodeIndexTo) {
        const TActorId proxy = GetInterconnectProxy(nodeIndexFrom, nodeIndexTo);

        Send(proxy, TActorId(), new TEvInterconnect::TEvConnectNode(), 0, 0, nodeIndexFrom, true);
        WaitForEvent(TEvInterconnect::EvNodeConnected);
    }

    void Disconnect(ui32 nodeIndexFrom, ui32 nodeIndexTo) {
        const TActorId proxy = GetInterconnectProxy(nodeIndexFrom, nodeIndexTo);

        Send(proxy, TActorId(), new TEvInterconnect::TEvDisconnect(), 0, 0, nodeIndexFrom, true);
        WaitForEvent(TEvInterconnect::EvNodeDisconnected);
    }

    template <typename TEvent>
    size_t CountEvents(bool dispatch = true) {
        if (dispatch) {
            if (!DispatchEvents()) {
                return 0;
            }
        }

        return CountIf(CaptureEvents(), [](const TAutoPtr<IEventHandle> ev) {
            return ev->GetTypeRewrite() == TEvent::EventType;
        });
    }

    template <typename TEvent>
    size_t CountEdgeEvents() {
        return CountEvents<TEvent>(false);
    }

    NInternalEvents::TEvHandshakeResponse::TPtr HandshakeReplica(
        const TActorId& replica,
        const TActorId& sender,
        ui64 owner = 1,
        ui64 generation = 1,
        bool grabResponse = true
    ) {
        Send(replica, sender, new NInternalEvents::TEvHandshakeRequest(owner, generation));

        if (grabResponse) {
            return GrabEdgeEvent<NInternalEvents::TEvHandshakeResponse>(sender);
        }

        return nullptr;
    }

    void CommitReplica(
        const TActorId& replica,
        const TActorId& sender,
        ui64 owner = 1,
        ui64 generation = 1
    ) {
        Send(replica, sender, new NInternalEvents::TEvCommitRequest(owner, generation));
    }

    template <typename TPath>
    NInternalEvents::TEvNotify::TPtr SubscribeReplica(
        const TActorId& replica,
        const TActorId& sender,
        const TPath& path,
        bool grabResponse = true,
        const ui64 domainOwnerId = 0,
        const NKikimrSchemeBoard::TEvSubscribe::TCapabilities& capabilities = NKikimrSchemeBoard::TEvSubscribe::TCapabilities()
    ) {
        auto subscribe = MakeHolder<NInternalEvents::TEvSubscribe>(path, domainOwnerId);
        subscribe->Record.MutableCapabilities()->CopyFrom(capabilities);

        Send(replica, sender, subscribe.Release());

        if (grabResponse) {
            return GrabEdgeEvent<NInternalEvents::TEvNotify>(sender);
        }

        return nullptr;
    }

    template <typename TPath>
    void UnsubscribeReplica(const TActorId& replica, const TActorId& sender, const TPath& path) {
        Send(replica, sender, new NInternalEvents::TEvUnsubscribe(path));
    }

    template <typename TEvent, typename TPath>
    TActorId CreateSubscriber(
        const TActorId& owner,
        const TPath& path,
        ui64 domainOwnerId = 1,
        bool grabResponse = true,
        ui32 nodeIndex = 0
    ) {
        const TActorId subscriber = Register(
            CreateSchemeBoardSubscriber(owner, path, domainOwnerId), nodeIndex
        );
        EnableScheduleForActor(subscriber, true);

        if (grabResponse) {
            GrabEdgeEvent<TEvent>(owner);
        }

        return subscriber;
    }

    template <typename TPath>
    TActorId CreateSubscriber(
        const TActorId& owner,
        const TPath& path,
        ui64 domainOwnerId = 1,
        ui32 nodeIndex = 0
    ) {
        return CreateSubscriber<NInternalEvents::TEvNotify>(
            owner, path, domainOwnerId, false, nodeIndex
        );
    }

}; // TTestContext

class TTestWithSchemeshard: public NUnitTest::TTestBase {
    static void AddDomain(
        TTestActorRuntime& runtime,
        TAppPrepare& app,
        const TString& name,
        ui32 domainUid,
        ui64 hiveTabletId,
        ui64 schemeshardTabletId
    ) {
        app.ClearDomainsAndHive();
        ui32 planResolution = 50;
        auto domain = TDomainsInfo::TDomain::ConstructDomainWithExplicitTabletIds(
            name, domainUid, schemeshardTabletId,
            planResolution,
            TVector<ui64>{TDomainsInfo::MakeTxCoordinatorIDFixed(1)},
            TVector<ui64>{},
            TVector<ui64>{TDomainsInfo::MakeTxAllocatorIDFixed(1)},
            DefaultPoolKinds(2)
        );

        TVector<ui64> ids = runtime.GetTxAllocatorTabletIds();
        ids.insert(ids.end(), domain->TxAllocators.begin(), domain->TxAllocators.end());
        runtime.SetTxAllocatorTabletIds(ids);

        app.AddDomain(domain.Release());
        app.AddHive(hiveTabletId);
    }

    static void SetupRuntime(TTestActorRuntime& runtime) {
        for (ui32 i : xrange(runtime.GetNodeCount())) {
            SetupStateStorage(runtime, i, 0);
        }

        TAppPrepare app;
        AddDomain(runtime, app, "Root", 0, TTestTxConfig::Hive, TTestTxConfig::SchemeShard);
        SetupChannelProfiles(app, 1);
        SetupTabletServices(runtime, &app, true);
    }

    static void BootSchemeShard(TTestActorRuntime& runtime, ui64 tabletId) {
        using namespace NSchemeShard;

        CreateTestBootstrapper(runtime, CreateTestTabletInfo(tabletId, TTabletTypes::SchemeShard), &CreateFlatTxSchemeShard);

        const TActorId edge = runtime.AllocateEdgeActor();

        auto init = new TEvSchemeShard::TEvInitRootShard(edge, 32, "Root");
        runtime.SendToPipe(tabletId, edge, init, 0, GetPipeConfigWithRetries());
        auto ev = runtime.GrabEdgeEvent<TEvSchemeShard::TEvInitRootShardResult>(edge);

        UNIT_ASSERT(ev->Get());
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetOrigin(), tabletId);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(), (ui32)TEvSchemeShard::TEvInitRootShardResult::StatusAlreadyInitialized);
    }

    static void BootTxAllocator(TTestActorRuntime& runtime, ui64 tabletId) {
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(tabletId, TTabletTypes::TxAllocator), &CreateTxAllocator);
    }

    void BootCoordinator(TTestActorRuntime& runtime, ui64 tabletId) {
        CoordinatorState = new TFakeCoordinator::TState();
        BootFakeCoordinator(runtime, tabletId, CoordinatorState);
    }

    void BootHive(TTestActorRuntime& runtime, ui64 tabletId) {
        HiveState = new TFakeHiveState();
        BootFakeHive(runtime, tabletId, HiveState);
    }

protected:
    virtual TTestContext::TEventObserver ObserverFunc() {
        return TTestContext::DefaultObserverFunc;
    }

public:
    void SetUp() override {
        Context = MakeHolder<TTestContext>();
        Context->SetObserverFunc(ObserverFunc());

        SetupRuntime(*Context);
        BootSchemeShard(*Context, TTestTxConfig::SchemeShard);
        BootTxAllocator(*Context, TTestTxConfig::TxAllocator);
        BootCoordinator(*Context, TTestTxConfig::Coordinator);
        BootHive(*Context, TTestTxConfig::Hive);
    }

    void TurnOnTabletsScheduling() {
        if (SchedulingGuard) {
            return;
        }

        TActorId sender = Context->AllocateEdgeActor();
        TVector<ui64> tabletIds;
        tabletIds.push_back((ui64)TTestTxConfig::SchemeShard);
        for (auto x: xrange(TTestTxConfig::FakeHiveTablets,  TTestTxConfig::FakeHiveTablets + 10)) {
            tabletIds.push_back(x);
        }

        SchedulingGuard = CreateTabletScheduledEventsGuard(tabletIds, *Context, sender);

        // make schemeShard visible for ScheduledEventsGuard
        // trigger actor resolving for existed tablet
        RebootTablet(*Context, (ui64)TTestTxConfig::SchemeShard, sender);
    }

    void TearDown() override {
        SchedulingGuard.Reset();
        CoordinatorState.Drop();
        HiveState.Drop();
        Context.Reset();
    }

protected:
    THolder<TTestContext> Context;

private:
    TFakeCoordinator::TState::TPtr CoordinatorState;
    TFakeHiveState::TPtr HiveState;
    THolder<ITabletScheduledEventsGuard> SchedulingGuard;

}; // TTestWithSchemeshard

NKikimrScheme::TEvDescribeSchemeResult GenerateDescribe(
    const TString& path,
    TPathId pathId,
    ui64 version = 1,
    TDomainId domainId = TDomainId()
);

NInternalEvents::TEvUpdate* GenerateUpdate(
    const NKikimrScheme::TEvDescribeSchemeResult& describe,
    ui64 owner = 1,
    ui64 generation = 1,
    bool isDeletion = false
);

struct TCombinationsArgs {
    TString Path;
    TPathId PathId;
    ui64 Version;
    TDomainId DomainId;

    ui64 OwnerId;
    ui64 Generation;
    bool IsDeletion;

    NInternalEvents::TEvUpdate* GenerateUpdate() const {
        return ::NKikimr::NSchemeBoard::GenerateUpdate(GenerateDescribe(), OwnerId, Generation, IsDeletion);
    }

    NKikimrScheme::TEvDescribeSchemeResult GenerateDescribe() const {
        return ::NKikimr::NSchemeBoard::GenerateDescribe(Path, PathId, Version, DomainId);
    }

    using TSuperId = std::tuple<TDomainId, bool, TPathId, ui64>;

    TSuperId GetSuperId() const {
        return {DomainId, IsDeletion, PathId, Version};
    }
};

TVector<TCombinationsArgs> GenerateCombinationsDomainRoot(TString path = TString("/Root/Tenant"), ui64 gssOwnerID = 800, TVector<ui64> tenantsOwners = TVector<ui64>{900, 910});
TVector<TCombinationsArgs> GenerateCombinationsMigratedPath(TString path,
                                                            ui64 gssID, TVector<ui64> tssIDs,
                                                            ui64 gssLocalPathId, ui64 tssLocalPathId);

} // NSchemeBoard
} // NKikimr
