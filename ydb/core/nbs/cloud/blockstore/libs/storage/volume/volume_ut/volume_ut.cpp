#include <ydb/core/nbs/cloud/blockstore/libs/storage/volume/volume_actor.h>

#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

// Partition tablet that accepts UpdateVolumeConfig and replies OK.
class TFakePartitionActor final
    : public TActor<TFakePartitionActor>
    , public NTabletFlatExecutor::TTabletExecutedFlat
{
public:
    TFakePartitionActor(const TActorId& tablet, TTabletStorageInfo* info)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
    {}

private:
    void StateInit(TAutoPtr<IEventHandle>& ev)
    {
        StateInitImpl(ev, SelfId());
    }

    void OnActivateExecutor(const TActorContext& ctx) override
    {
        Become(&TThis::StateWork);
        SignalTabletActive(ctx);
    }

    void OnDetach(const TActorContext& ctx) override
    {
        Die(ctx);
    }

    void OnTabletDead(
        TEvTablet::TEvTabletDead::TPtr& ev,
        const TActorContext& ctx) override
    {
        Y_UNUSED(ev);
        Die(ctx);
    }

    void DefaultSignalTabletActive(const TActorContext&) override
    {}

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(
                TEvBlockStore::TEvUpdateVolumeConfig,
                HandleUpdateVolumeConfig);
            default:
                HandleDefaultEvents(ev, SelfId());
                break;
        }
    }

    void HandleUpdateVolumeConfig(
        TEvBlockStore::TEvUpdateVolumeConfig::TPtr& ev,
        const TActorContext& ctx)
    {
        auto response =
            std::make_unique<TEvBlockStore::TEvUpdateVolumeConfigResponse>();
        response->Record.SetTxId(ev->Get()->Record.GetTxId());
        response->Record.SetOrigin(TabletID());
        response->Record.SetStatus(NKikimrBlockStore::OK);
        ctx.Send(ev->Sender, response.release());
    }
};

////////////////////////////////////////////////////////////////////////////////

// Boots a volume and one fake partition, and counts config forwards.
class TVolumeTestEnv
{
public:
    static constexpr ui64 VolumeTabletId = MakeTabletID(false, 1);
    static constexpr ui64 PartitionTabletId = MakeTabletID(false, 2);

    // Boots the volume tablet and its fake partition tablet.
    TVolumeTestEnv()
    {
        SetupTabletServices(Runtime);
        Runtime.SetLogPriority(NKikimrServices::NBS_VOLUME, NLog::PRI_DEBUG);

        BootObserver = Runtime.AddObserver<TEvTablet::TEvBoot>(
            [this](TEvTablet::TEvBoot::TPtr& ev)
            {
                if (ev->Get()->TabletID == PartitionTabletId) {
                    PartitionActor = ev->GetRecipientRewrite();
                }
            });

        ForwardObserver =
            Runtime.AddObserver<TEvBlockStore::TEvUpdateVolumeConfig>(
                [this](TEvBlockStore::TEvUpdateVolumeConfig::TPtr& ev)
                {
                    // Pipe delivery rewrites the recipient; Recipient stays the
                    // pipe server.
                    if (!PartitionActor ||
                        ev->GetRecipientRewrite() != PartitionActor) {
                        return;
                    }

                    ++Deliveries;
                    if (DropDeliveries == 0) {
                        return;
                    }

                    --DropDeliveries;
                    ev.Reset();
                });

        ConnectObserver =
            Runtime.AddObserver<TEvTabletPipe::TEvClientConnected>(
                [this](TEvTabletPipe::TEvClientConnected::TPtr& ev)
                {
                    const auto* msg = ev->Get();
                    if (!FailNextPartitionConnect ||
                        msg->TabletId != PartitionTabletId ||
                        msg->Status != NKikimrProto::OK)
                    {
                        return;
                    }

                    FailNextPartitionConnect = false;
                    Runtime.Schedule(
                        new IEventHandle(
                            ev->Recipient,
                            ev->Sender,
                            new TEvTabletPipe::TEvClientConnected(
                                msg->TabletId,
                                NKikimrProto::ERROR,
                                msg->ClientId,
                                msg->ServerId,
                                msg->Leader,
                                msg->Dead,
                                msg->Generation)),
                        TDuration::MilliSeconds(1));
                    ev.Reset();
                });

        Boot(
            VolumeTabletId,
            TTabletTypes::BlockStoreVolumeDirect,
            [](const TActorId& tablet, TTabletStorageInfo* info) -> IActor*
            { return new TVolumeActor(tablet, info); });
        Boot(
            PartitionTabletId,
            TTabletTypes::BlockStorePartitionDirect,
            [](const TActorId& tablet, TTabletStorageInfo* info) -> IActor*
            { return new TFakePartitionActor(tablet, info); });
        UNIT_ASSERT(PartitionActor);
    }

    // Sends UpdateVolumeConfig to the volume as SchemeShard would.
    TActorId SendUpdate(ui64 txId)
    {
        const TActorId edge = Runtime.AllocateEdgeActor();

        auto request = std::make_unique<TEvBlockStore::TEvUpdateVolumeConfig>();
        request->Record.SetTxId(txId);
        auto* partition = request->Record.AddPartitions();
        partition->SetPartitionId(0);
        partition->SetTabletId(PartitionTabletId);

        Runtime.SendToPipe(VolumeTabletId, edge, request.release());
        return edge;
    }

    // Waits until the partition has been offered count config forwards.
    void WaitForDeliveries(ui32 count)
    {
        if (Deliveries >= count) {
            return;
        }

        TDispatchOptions options;
        options.CustomFinalCondition = [this, count]
        {
            return Deliveries >= count;
        };
        options.FinalEvents.emplace_back([](IEventHandle&) { return false; });
        Runtime.DispatchEvents(options);
        UNIT_ASSERT_C(Deliveries >= count, Deliveries);
    }

    // Returns the volume reply delivered to edge.
    NKikimrBlockStore::TUpdateVolumeConfigResponse GrabResponse(
        const TActorId& edge)
    {
        auto response =
            Runtime.GrabEdgeEvent<TEvBlockStore::TEvUpdateVolumeConfigResponse>(
                edge);
        UNIT_ASSERT(response);
        return response->Get()->Record;
    }

    // Restarts the partition tablet.
    void RebootPartition()
    {
        const TActorId edge = Runtime.AllocateEdgeActor();
        RebootTablet(Runtime, PartitionTabletId, edge);
    }

    // Runs the actor system for timeout of simulated time.
    void DispatchFor(TDuration timeout)
    {
        TDispatchOptions options;
        options.FinalEvents.emplace_back([](IEventHandle&) { return false; });
        Runtime.DispatchEvents(options, timeout);
    }

    TTestBasicRuntime Runtime;
    TActorId PartitionActor;
    ui32 Deliveries = 0;
    ui32 DropDeliveries = 0;
    bool FailNextPartitionConnect = false;

private:
    void Boot(
        ui64 tabletId,
        TTabletTypes::EType type,
        std::function<IActor*(const TActorId&, TTabletStorageInfo*)> factory)
    {
        CreateTestBootstrapper(
            Runtime,
            CreateTestTabletInfo(tabletId, type),
            std::move(factory));

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvTablet::EvBoot, 1);
        Runtime.DispatchEvents(options);
    }

    TTestActorRuntime::TEventObserverHolder BootObserver;
    TTestActorRuntime::TEventObserverHolder ForwardObserver;
    TTestActorRuntime::TEventObserverHolder ConnectObserver;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeTest)
{
    Y_UNIT_TEST(ShouldForwardUpdateVolumeConfigToPartition)
    {
        TVolumeTestEnv env;

        const TActorId edge = env.SendUpdate(1);
        const auto response = env.GrabResponse(edge);

        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(response.GetStatus()),
            static_cast<int>(NKikimrBlockStore::OK));
        UNIT_ASSERT_VALUES_EQUAL(response.GetTxId(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            response.GetOrigin(),
            TVolumeTestEnv::VolumeTabletId);
        UNIT_ASSERT_VALUES_EQUAL(env.Deliveries, 1);
    }

    Y_UNIT_TEST(ShouldResendUpdateVolumeConfigWhenPartitionPipeIsDestroyed)
    {
        TVolumeTestEnv env;
        env.DropDeliveries = 1;

        const TActorId edge = env.SendUpdate(1);
        env.WaitForDeliveries(1);

        env.RebootPartition();

        const auto response = env.GrabResponse(edge);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(response.GetStatus()),
            static_cast<int>(NKikimrBlockStore::OK));
        UNIT_ASSERT_VALUES_EQUAL(response.GetTxId(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            response.GetOrigin(),
            TVolumeTestEnv::VolumeTabletId);
        UNIT_ASSERT_VALUES_EQUAL(env.Deliveries, 2);
    }

    Y_UNIT_TEST(ShouldResendUpdateVolumeConfigWhenPartitionPipeConnectFails)
    {
        TVolumeTestEnv env;
        env.DropDeliveries = 1;
        env.FailNextPartitionConnect = true;

        const TActorId edge = env.SendUpdate(1);
        const auto response = env.GrabResponse(edge);

        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(response.GetStatus()),
            static_cast<int>(NKikimrBlockStore::OK));
        UNIT_ASSERT_VALUES_EQUAL(response.GetTxId(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            response.GetOrigin(),
            TVolumeTestEnv::VolumeTabletId);
        UNIT_ASSERT_VALUES_EQUAL(env.Deliveries, 2);
    }

    Y_UNIT_TEST(ShouldNotResendAfterPipeIsClosed)
    {
        TVolumeTestEnv env;

        const TActorId first = env.SendUpdate(1);
        const auto firstResponse = env.GrabResponse(first);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(firstResponse.GetStatus()),
            static_cast<int>(NKikimrBlockStore::OK));
        UNIT_ASSERT_VALUES_EQUAL(env.Deliveries, 1);

        env.RebootPartition();
        env.DispatchFor(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(env.Deliveries, 1);

        const TActorId second = env.SendUpdate(2);
        const auto secondResponse = env.GrabResponse(second);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(secondResponse.GetStatus()),
            static_cast<int>(NKikimrBlockStore::OK));
        UNIT_ASSERT_VALUES_EQUAL(secondResponse.GetTxId(), 2);
        UNIT_ASSERT_VALUES_EQUAL(
            secondResponse.GetOrigin(),
            TVolumeTestEnv::VolumeTabletId);
        UNIT_ASSERT_VALUES_EQUAL(env.Deliveries, 2);
    }

    Y_UNIT_TEST(ShouldReplyToLatestSenderOnRepeatedTxId)
    {
        TVolumeTestEnv env;
        env.DropDeliveries = 1;

        const TActorId firstSender = env.SendUpdate(1);
        env.WaitForDeliveries(1);

        const TActorId latestSender = env.SendUpdate(1);
        env.RebootPartition();

        const auto response = env.GrabResponse(latestSender);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(response.GetStatus()),
            static_cast<int>(NKikimrBlockStore::OK));
        UNIT_ASSERT_VALUES_EQUAL(response.GetTxId(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            response.GetOrigin(),
            TVolumeTestEnv::VolumeTabletId);

        auto firstResponse =
            env.Runtime
                .GrabEdgeEvent<TEvBlockStore::TEvUpdateVolumeConfigResponse>(
                    firstSender,
                    TDuration::Seconds(1));
        UNIT_ASSERT(!firstResponse);
        UNIT_ASSERT_VALUES_EQUAL(env.Deliveries, 2);
    }

    Y_UNIT_TEST(ShouldResendAllPendingEventsWhenPartitionPipeFails)
    {
        TVolumeTestEnv env;
        env.DropDeliveries = 1;

        const TActorId firstSender = env.SendUpdate(1);
        env.WaitForDeliveries(1);

        const TActorId secondSender = env.SendUpdate(2);
        const auto secondResponse = env.GrabResponse(secondSender);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(secondResponse.GetStatus()),
            static_cast<int>(NKikimrBlockStore::OK));
        UNIT_ASSERT_VALUES_EQUAL(secondResponse.GetTxId(), 2);
        UNIT_ASSERT_VALUES_EQUAL(
            secondResponse.GetOrigin(),
            TVolumeTestEnv::VolumeTabletId);

        env.RebootPartition();

        const auto firstResponse = env.GrabResponse(firstSender);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(firstResponse.GetStatus()),
            static_cast<int>(NKikimrBlockStore::OK));
        UNIT_ASSERT_VALUES_EQUAL(firstResponse.GetTxId(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            firstResponse.GetOrigin(),
            TVolumeTestEnv::VolumeTabletId);
        UNIT_ASSERT_VALUES_EQUAL(env.Deliveries, 3);
    }
}

}   // namespace NYdb::NBS::NStorage
