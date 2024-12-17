#include "bootstrapper.h"
#include "bootstrapper_impl.h"
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(BootstrapperTest) {

    class TSimpleTablet
        : public TActor<TSimpleTablet>
        , public NTabletFlatExecutor::TTabletExecutedFlat
    {
    public:
        TSimpleTablet(const TActorId &tablet, TTabletStorageInfo *info)
            : TActor(&TThis::StateInit)
            , TTabletExecutedFlat(info, tablet, nullptr)
        {}

    private:
        void DefaultSignalTabletActive(const TActorContext&) override {
            // must be empty
        }

        void OnActivateExecutor(const TActorContext& ctx) override {
            Become(&TThis::StateWork);
            SignalTabletActive(ctx);
        }

        void OnDetach(const TActorContext &ctx) override {
            return Die(ctx);
        }

        void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &, const TActorContext &ctx) override {
            return Die(ctx);
        }

        void Handle(TEvents::TEvPing::TPtr& ev) {
            Send(ev->Sender, new TEvents::TEvPong);
        }

        STFUNC(StateInit) {
            StateInitImpl(ev, SelfId());
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvents::TEvPing, Handle);
            default:
                HandleDefaultEvents(ev, SelfId());
            }
        }
    };

    TIntrusivePtr<TTabletStorageInfo> CreateSimpleTabletStorageInfo(ui64 tabletId = TTestTxConfig::TxTablet0) {
        return CreateTestTabletInfo(tabletId, TTabletTypes::Dummy);
    }

    TIntrusivePtr<TTabletSetupInfo> CreateSimpleTabletSetupInfo() {
        return MakeIntrusive<TTabletSetupInfo>(
            [](const TActorId& tablet, TTabletStorageInfo* info) {
                return new TSimpleTablet(tablet, info);
            },
            TMailboxType::Simple,
            ui32(0),
            TMailboxType::Simple,
            ui32(0));
    }

    TActorId StartSimpleTablet(
            TTestActorRuntime& runtime,
            const TActorId& launcher = {},
            ui32 nodeIdx = 0,
            ui64 tabletId = TTestTxConfig::TxTablet0)
    {
        auto tabletInfo = CreateSimpleTabletStorageInfo(tabletId);
        auto setupInfo = CreateSimpleTabletSetupInfo();
        auto actor = runtime.Register(
            CreateTablet(launcher, tabletInfo.Get(), setupInfo.Get(), /* generation */ 0),
            nodeIdx);
        runtime.EnableScheduleForActor(actor);
        return actor;
    }

    std::vector<TActorId> StartSimpleTabletBootstrappers(
            TTestActorRuntime& runtime,
            const std::vector<ui32>& nodeIdxs,
            ui64 tabletId = TTestTxConfig::TxTablet0)
    {
        std::vector<TActorId> boots;
        auto tabletInfo = CreateSimpleTabletStorageInfo(tabletId);
        auto setupInfo = CreateSimpleTabletSetupInfo();
        auto bootInfo = MakeIntrusive<TBootstrapperInfo>(setupInfo.Get());
        for (ui32 nodeIdx : nodeIdxs) {
            bootInfo->Nodes.push_back(runtime.GetNodeId(nodeIdx));
        }
        THashMap<ui32, TActorId> started;
        for (ui32 nodeIdx : nodeIdxs) {
            if (started.contains(nodeIdx)) {
                // Start one bootstrapper per node
                boots.push_back(started.at(nodeIdx));
                continue;
            }
            boots.push_back(runtime.Register(CreateBootstrapper(tabletInfo.Get(), bootInfo.Get()), nodeIdx));
            runtime.EnableScheduleForActor(boots.back());
            // Make this bootstrapper discoverable by tablet id / node id
            runtime.RegisterService(
                MakeBootstrapperID(tabletId, runtime.GetNodeId(nodeIdx)),
                boots.back(),
                nodeIdx);
            started[nodeIdx] = boots.back();
        }
        return boots;
    }

    TActorId StartSimpleTabletBootstrapper(
            TTestActorRuntime& runtime,
            ui32 nodeIdx = 0,
            ui64 tabletId = TTestTxConfig::TxTablet0)
    {
        return StartSimpleTabletBootstrappers(runtime, {nodeIdx}, tabletId).at(0);
    }

    Y_UNIT_TEST(LoneBootstrapper) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);
        auto sender = runtime.AllocateEdgeActor();

        StartSimpleTabletBootstrapper(runtime);

        ui32 gen1;
        auto client = runtime.ConnectToPipe(TTestTxConfig::TxTablet0, sender, 0, NTabletPipe::TClientRetryPolicy::WithRetries());
        {
            Cerr << "... waiting for pipe to connect" << Endl;
            auto ev = runtime.GrabEdgeEventRethrow<TEvTabletPipe::TEvClientConnected>(sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
            gen1 = ev->Get()->Generation;
        }
        Cerr << "... stopping current instance" << Endl;
        runtime.SendToPipe(client, sender, new TEvents::TEvPoison);
        {
            Cerr << "... waiting for pipe to disconnect" << Endl;
            auto ev = runtime.GrabEdgeEventRethrow<TEvTabletPipe::TEvClientDestroyed>(sender);
        }

        ui32 gen2;
        client = runtime.ConnectToPipe(TTestTxConfig::TxTablet0, sender, 0, NTabletPipe::TClientRetryPolicy::WithRetries());
        {
            Cerr << "... waiting for pipe to connect" << Endl;
            auto ev = runtime.GrabEdgeEventRethrow<TEvTabletPipe::TEvClientConnected>(sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
            gen2 = ev->Get()->Generation;
        }

        UNIT_ASSERT_C(gen1 < gen2, "Unexpected gen1# " << gen1 << " not before gen2# " << gen2);
    }

    Y_UNIT_TEST(KeepExistingTablet) {
        TTestBasicRuntime runtime(2);
        SetupTabletServices(runtime);

        auto launcher = runtime.AllocateEdgeActor(1);
        auto instance = StartSimpleTablet(runtime, launcher, 1);

        bool dead = false;
        auto deadObserver = runtime.AddObserver<TEvTablet::TEvTabletDead>(
            [&](TEvTablet::TEvTabletDead::TPtr& ev) {
                if (ev->Sender == instance) {
                    dead = true;
                }
            });

        auto sender = runtime.AllocateEdgeActor();

        ui32 gen1;
        auto client = runtime.ConnectToPipe(TTestTxConfig::TxTablet0, sender, 0, NTabletPipe::TClientRetryPolicy::WithRetries());
        {
            Cerr << "... waiting for pipe to connect" << Endl;
            auto ev = runtime.GrabEdgeEventRethrow<TEvTabletPipe::TEvClientConnected>(sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
            gen1 = ev->Get()->Generation;
        }

        StartSimpleTabletBootstrapper(runtime);

        Cerr << "... sleeping (original instance should be preserved)" << Endl;
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_C(!dead, "The original instance should be preserved");

        runtime.Send(new IEventHandle(instance, launcher, new TEvents::TEvPoison), 1);
        runtime.WaitFor("original instance to stop", [&]{ return dead; });

        ui32 gen2;
        client = runtime.ConnectToPipe(TTestTxConfig::TxTablet0, sender, 0, NTabletPipe::TClientRetryPolicy::WithRetries());
        {
            Cerr << "... waiting for pipe to connect" << Endl;
            auto ev = runtime.GrabEdgeEventRethrow<TEvTabletPipe::TEvClientConnected>(sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
            gen2 = ev->Get()->Generation;
        }

        UNIT_ASSERT_C(gen1 < gen2, "Unexpected gen1# " << gen1 << " not before gen2# " << gen2);
    }

    Y_UNIT_TEST(RestartUnavailableTablet) {
        TTestBasicRuntime runtime(3);
        SetupTabletServices(runtime);

        auto launcher = runtime.AllocateEdgeActor(1);
        auto instance = StartSimpleTablet(runtime, launcher, 1);
        Y_UNUSED(instance);

        auto sender = runtime.AllocateEdgeActor();

        ui32 gen1;
        auto client = runtime.ConnectToPipe(TTestTxConfig::TxTablet0, sender, 0, NTabletPipe::TClientRetryPolicy::WithRetries());
        {
            Cerr << "... waiting for pipe to connect" << Endl;
            auto ev = runtime.GrabEdgeEventRethrow<TEvTabletPipe::TEvClientConnected>(sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->ServerId.NodeId(), runtime.GetNodeId(1));
            gen1 = ev->Get()->Generation;
        }

        TBlockEvents<TEvTabletPipe::TEvConnect> blockedConnect(runtime,
            [&](const auto& ev) {
                return ev->Get()->Record.GetTabletId() == TTestTxConfig::TxTablet0;
            });

        StartSimpleTabletBootstrapper(runtime, 2);

        runtime.WaitFor("blocked connect attempt", [&]{ return blockedConnect.size() >= 1; });
        blockedConnect.Stop();

        Cerr << "... disconnecting nodes 2 <-> 1" << Endl;
        runtime.DisconnectNodes(2, 1);

        {
            Cerr << "... waiting for pipe to disconnect" << Endl;
            auto ev = runtime.GrabEdgeEventRethrow<TEvTabletPipe::TEvClientDestroyed>(sender);
        }

        ui32 gen2;
        client = runtime.ConnectToPipe(TTestTxConfig::TxTablet0, sender, 0, NTabletPipe::TClientRetryPolicy::WithRetries());
        {
            Cerr << "... waiting for pipe to connect" << Endl;
            auto ev = runtime.GrabEdgeEventRethrow<TEvTabletPipe::TEvClientConnected>(sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->ServerId.NodeId(), runtime.GetNodeId(2));
            gen2 = ev->Get()->Generation;
        }

        UNIT_ASSERT_C(gen1 < gen2, "Unexpected gen1# " << gen1 << " not before gen2# " << gen2);
    }

    Y_UNIT_TEST(UnavailableStateStorage) {
        TTestBasicRuntime runtime(3);
        SetupTabletServices(runtime);
        runtime.SetLogPriority(NKikimrServices::BOOTSTRAPPER, NActors::NLog::PRI_DEBUG);

        auto launcher = runtime.AllocateEdgeActor(1);
        auto instance = StartSimpleTablet(runtime, launcher, 1);

        bool dead = false;
        auto deadObserver = runtime.AddObserver<TEvTablet::TEvTabletDead>(
            [&](TEvTablet::TEvTabletDead::TPtr& ev) {
                if (ev->Sender == instance) {
                    dead = true;
                }
            });

        auto sender = runtime.AllocateEdgeActor();

        ui32 gen1;
        auto client = runtime.ConnectToPipe(TTestTxConfig::TxTablet0, sender, 0, NTabletPipe::TClientRetryPolicy::WithRetries());
        {
            Cerr << "... waiting for pipe to connect" << Endl;
            auto ev = runtime.GrabEdgeEventRethrow<TEvTabletPipe::TEvClientConnected>(sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->ServerId.NodeId(), runtime.GetNodeId(1));
            gen1 = ev->Get()->Generation;
        }

        ui32 node2 = runtime.GetNodeId(2);
        TBlockEvents<TEvStateStorage::TEvReplicaLookup> blockedReplicaLookup(runtime,
            [&](const auto& ev) {
                // Block EvReplicaLookup requests from node 2 and disconnect every time
                if (ev->Sender.NodeId() == node2 && ev->Get()->Record.GetTabletID() == TTestTxConfig::TxTablet0) {
                    Cerr << "... disconnecting nodes 2 <-> 0 (" << ev->Get()->ToString() << " for " << ev->GetRecipientRewrite() << ")" << Endl;
                    runtime.DisconnectNodes(2, 0);
                    return true;
                }
                return false;
            });

        StartSimpleTabletBootstrapper(runtime, 2);
        runtime.WaitFor("multiple state storage lookup attempts", [&]{ return blockedReplicaLookup.size() >= 6; });

        UNIT_ASSERT_C(!dead, "The original instance should be preserved");

        Y_UNUSED(client);
        Y_UNUSED(gen1);
    }

    Y_UNIT_TEST(MultipleBootstrappers) {
        TTestBasicRuntime runtime(4);
        SetupTabletServices(runtime);
        runtime.SetLogPriority(NKikimrServices::BOOTSTRAPPER, NActors::NLog::PRI_DEBUG);

        StartSimpleTabletBootstrappers(runtime, {1, 2, 3});

        Cerr << "... sleeping for 2 seconds" << Endl;
        runtime.SimulateSleep(TDuration::Seconds(2));

        auto sender = runtime.AllocateEdgeActor();

        ui32 gen1;
        ui32 initialNode;
        ui32 initialNodeIdx;
        auto client = runtime.ConnectToPipe(TTestTxConfig::TxTablet0, sender, 0, NTabletPipe::TClientRetryPolicy::WithRetries());
        {
            Cerr << "... waiting for pipe to connect" << Endl;
            auto ev = runtime.GrabEdgeEventRethrow<TEvTabletPipe::TEvClientConnected>(sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
            gen1 = ev->Get()->Generation;
            initialNode = ev->Get()->ServerId.NodeId();
            initialNodeIdx = initialNode - runtime.GetNodeId(0);
        }

        Cerr << "... tablet initially started on node " << initialNode
            << " (idx " << initialNodeIdx << ")" << " in gen " << gen1 << Endl;
        UNIT_ASSERT_VALUES_EQUAL_C(gen1, 2u, "Expected tablet to start from gen 2");

        std::vector<ui32> otherNodeIdxs;
        for (ui32 nodeIdx : {1, 2, 3}) {
            if (nodeIdx != initialNodeIdx) {
                otherNodeIdxs.push_back(nodeIdx);
            }
        }

        Cerr << "... disconnecting other nodes" << Endl;
        for (ui32 nodeIdx : otherNodeIdxs) {
            runtime.DisconnectNodes(initialNodeIdx, nodeIdx);
        }

        Cerr << "... sleeping for 2 seconds (tablet expected to survive)" << Endl;
        runtime.SimulateSleep(TDuration::Seconds(2));

        {
            runtime.SendToPipe(client, sender, new TEvents::TEvPing);
            runtime.GrabEdgeEventRethrow<TEvents::TEvPong>(sender);
        }

        // Block new connect attempts to tablet at that node
        TBlockEvents<TEvTabletPipe::TEvConnect> blockConnect(runtime,
            [&](const auto& ev) {
                if (ev->GetRecipientRewrite().NodeId() == initialNode &&
                    ev->Get()->Record.GetTabletId() == TTestTxConfig::TxTablet0)
                {
                    ui32 otherNodeIdx = ev->Sender.NodeId() - runtime.GetNodeId(0);
                    Cerr << "... disconnecting nodes " << initialNodeIdx << " <-> " << otherNodeIdx
                        << " (tablet connect attempt)" << Endl;
                    runtime.DisconnectNodes(initialNodeIdx, otherNodeIdx);
                    return true;
                }
                return false;
            });

        Cerr << "... disconnecting other nodes (new tablet connections fail)" << Endl;
        for (ui32 nodeIdx : otherNodeIdxs) {
            runtime.DisconnectNodes(initialNodeIdx, nodeIdx);
        }

        Cerr << "... sleeping for 2 seconds (tablet expected to survive)" << Endl;
        runtime.SimulateSleep(TDuration::Seconds(2));

        {
            runtime.SendToPipe(client, sender, new TEvents::TEvPing);
            runtime.GrabEdgeEventRethrow<TEvents::TEvPong>(sender);
        }

        // Block watch messages towards tablet node, a new owner must be selected
        TBlockEvents<TEvBootstrapper::TEvWatch> blockWatch(runtime,
            [&](const auto& ev) {
                if (ev->GetRecipientRewrite().NodeId() == initialNode &&
                    ev->Get()->Record.GetTabletID() == TTestTxConfig::TxTablet0)
                {
                    ui32 otherNodeIdx = ev->Sender.NodeId() - runtime.GetNodeId(0);
                    Cerr << "... disconnecting nodes " << initialNodeIdx << " <-> " << otherNodeIdx
                        << " (bootstrap watch attempt)" << Endl;
                    runtime.DisconnectNodes(initialNodeIdx, otherNodeIdx);
                    return true;
                }
                return false;
            });

        Cerr << "... disconnect other nodes (new owner expected)" << Endl;
        for (ui32 nodeIdx : otherNodeIdxs) {
            runtime.DisconnectNodes(initialNodeIdx, nodeIdx);
        }

        Cerr << "... sleeping for 2 seconds (new tablet expected to start once)" << Endl;
        runtime.SimulateSleep(TDuration::Seconds(2));

        ui32 gen2;
        ui32 secondNode;
        ui32 secondNodeIdx;
        auto sender2 = runtime.AllocateEdgeActor();
        auto client2 = runtime.ConnectToPipe(TTestTxConfig::TxTablet0, sender2, 0, NTabletPipe::TClientRetryPolicy::WithRetries());
        {
            Cerr << "... waiting for pipe to connect" << Endl;
            auto ev = runtime.GrabEdgeEventRethrow<TEvTabletPipe::TEvClientConnected>(sender2);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
            gen2 = ev->Get()->Generation;
            secondNode = ev->Get()->ServerId.NodeId();
            secondNodeIdx = secondNode - runtime.GetNodeId(0);
        }

        UNIT_ASSERT_C(secondNodeIdx != initialNodeIdx, "Tablet expected to move to a different node");
        UNIT_ASSERT_C(gen2 == gen1 + 1, "Tablet restarted with gen2# " << gen2 << " after gen1# " << gen1);

        runtime.GrabEdgeEventRethrow<TEvTabletPipe::TEvClientDestroyed>(sender);
        Y_UNUSED(client2);
    }

    Y_UNIT_TEST(DuplicateNodes) {
        TTestBasicRuntime runtime(3);
        SetupTabletServices(runtime);
        runtime.SetLogPriority(NKikimrServices::BOOTSTRAPPER, NActors::NLog::PRI_DEBUG);

        StartSimpleTabletBootstrappers(runtime, {1, 1, 2, 2});

        size_t boots = 0;
        auto observer = runtime.AddObserver<TEvTablet::TEvBoot>([&](auto&) {
            ++boots;
        });

        runtime.SimulateSleep(TDuration::Seconds(1));

        // Tablet must boot exactly once
        UNIT_ASSERT_VALUES_EQUAL(boots, 1u);
    }

} // Y_UNIT_TEST_SUITE(BootstrapperTest)

} // namespace NKikimr
