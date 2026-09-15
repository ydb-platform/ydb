#include "../blob_depot.h"
#include "../events.h"
#include "../s3_router_events.h"
#include "../types.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tablet_types.h>
#include <ydb/core/protos/blob_depot_config.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/tx.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::NBlobDepot;

namespace {

    constexpr ui64 TabletId = TTestTxConfig::TxTablet0;
    constexpr ui32 VirtualGroupId = 0x80000001;
    constexpr ui64 UserTabletId = 1'000;

    class TBlackHoleActor : public TActor<TBlackHoleActor> {
    public:
        TBlackHoleActor()
            : TActor(&TThis::StateFunc)
        {}

        STFUNC(StateFunc) {
            Y_UNUSED(ev);
        }
    };

    struct TFakeAgent {
        TTestBasicRuntime& Runtime;
        const ui32 NodeIndex;
        const TActorId Edge;
        const ui64 AgentInstanceId;
        TActorId PipeClient;
        ui64 NextRequestId = 1;

        TFakeAgent(TTestBasicRuntime& runtime, ui64 agentInstanceId, ui32 nodeIndex = 0)
            : Runtime(runtime)
            , NodeIndex(nodeIndex)
            , Edge(runtime.AllocateEdgeActor(nodeIndex))
            , AgentInstanceId(agentInstanceId)
            , PipeClient(runtime.ConnectToPipe(TabletId, Edge, nodeIndex, GetPipeConfigWithRetries()))
        {}

        template<typename TRequest>
        void Send(std::unique_ptr<TRequest> ev) {
            Runtime.SendToPipe(PipeClient, Edge, ev.release(), NodeIndex, NextRequestId++);
        }

        template<typename TResponse>
        typename TResponse::TPtr Grab(TDuration simTimeout = TDuration::Seconds(30)) {
            return Runtime.GrabEdgeEvent<TResponse>(Edge, simTimeout);
        }

        void Register() {
            Send(std::make_unique<TEvBlobDepot::TEvRegisterAgent>(VirtualGroupId, AgentInstanceId));
            auto res = Grab<TEvBlobDepot::TEvRegisterAgentResult>();
            UNIT_ASSERT(res);
            UNIT_ASSERT(res->Get()->Record.HasS3BackendSettings());
        }

        void SendPrepareWriteS3(ui32 cookie, ui32 len = 100) {
            auto ev = std::make_unique<TEvBlobDepot::TEvPrepareWriteS3>();
            auto *item = ev->Record.AddItems();
            item->SetKey(TLogoBlobID(UserTabletId, 1, 1, 0, len, cookie).AsBinaryString());
            item->SetLen(len);
            Send(std::move(ev));
        }

        std::optional<TS3Locator> GrabPrepareWriteS3Result(TDuration simTimeout = TDuration::Seconds(30)) {
            auto res = Grab<TEvBlobDepot::TEvPrepareWriteS3Result>(simTimeout);
            if (!res) {
                return std::nullopt;
            }
            const auto& record = res->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(record.ItemsSize(), 1);
            const auto& item = record.GetItems(0);
            UNIT_ASSERT_VALUES_EQUAL_C(item.GetStatus(), NKikimrProto::OK, item.GetErrorReason());
            UNIT_ASSERT(item.HasS3Locator());
            return TS3Locator::FromProto(item.GetS3Locator());
        }

        TS3Locator PrepareWriteS3(ui32 cookie) {
            SendPrepareWriteS3(cookie);
            auto locator = GrabPrepareWriteS3Result();
            UNIT_ASSERT_C(locator, "TEvPrepareWriteS3 was not answered");
            return *locator;
        }

        void Discard(const TS3Locator& locator, bool slowDown) {
            auto ev = std::make_unique<TEvBlobDepot::TEvDiscardSpoiledBlobSeq>();
            locator.ToProto(ev->Record.AddS3Locators());
            if (slowDown) {
                ev->Record.SetS3SlowDown(true);
            }
            Send(std::move(ev));
        }

        void DiscardWithSlowDown(const TS3Locator& locator) {
            Discard(locator, /*slowDown=*/true);
        }

        void Disconnect() {
            Runtime.ClosePipe(PipeClient, Edge, NodeIndex);
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTabletPipe::EvServerDisconnected);
            Runtime.DispatchEvents(options);
        }
    };

    // While Active, every TEvPut issued by the tablet (redo log writes) is parked here instead of reaching the group,
    // so transactions get stuck between Execute and Complete until Release().
    struct TLogWriteHold {
        bool Active = false;
        std::vector<THolder<IEventHandle>> Held;

        void Release(TTestBasicRuntime& runtime) {
            Active = false;
            for (auto& ev : std::exchange(Held, {})) {
                runtime.Send(ev.Release(), 0, true);
            }
        }
    };

    void StartBlobDepotWithS3(TTestBasicRuntime& runtime, TLogWriteHold *logWriteHold = nullptr) {
        SetupTabletServices(runtime);

        runtime.SetObserverFunc([logWriteHold](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvNodeWardenAcquireBlobDepotS3Router) {
                return TTestActorRuntime::EEventAction::DROP;
            }
            if (logWriteHold && logWriteHold->Active && ev->GetTypeRewrite() == TEvBlobStorage::EvPut) {
                logWriteHold->Held.emplace_back(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        runtime.RegisterService(MakeBlobDepotS3RouterID(TabletId), runtime.Register(new TBlackHoleActor));

        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TabletId, TTabletTypes::BlobDepot, TErasureType::ErasureNone),
            &CreateBlobDepot);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTablet::EvBoot);
            runtime.DispatchEvents(options);
        }

        const TActorId edge = runtime.AllocateEdgeActor();
        auto ev = std::make_unique<TEvBlobDepot::TEvApplyConfig>(1);
        auto *config = ev->Record.MutableConfig();
        config->SetVirtualGroupId(VirtualGroupId);
        config->SetName("vg");
        {
            auto *prof = config->AddChannelProfiles();
            prof->SetCount(2);
        }
        {
            auto *prof = config->AddChannelProfiles();
            prof->SetChannelKind(NKikimrBlobDepot::TChannelKind::Data);
            prof->SetCount(3);
        }
        auto *s3 = config->MutableS3BackendSettings();
        s3->MutableSyncMode();
        auto *settings = s3->MutableSettings();
        settings->SetEndpoint("localhost:1");
        settings->SetScheme(NKikimrSchemeOp::TS3Settings::HTTP);
        settings->SetBucket("bucket");
        settings->SetObjectKeyPattern("prefix");
        runtime.SendToPipe(TabletId, edge, ev.release(), 0, GetPipeConfigWithRetries());
        auto res = runtime.GrabEdgeEvent<TEvBlobDepot::TEvApplyConfigResult>(edge);
        UNIT_ASSERT(res);
    }

} // namespace

Y_UNIT_TEST_SUITE(BlobDepotS3WriteThrottle) {

    Y_UNIT_TEST(SlotsAreReleasedOnAgentDisconnect) {
        TTestBasicRuntime runtime;
        StartBlobDepotWithS3(runtime);

        // Agent #1 allocates two locators and dies with them in flight (e.g. node restart / pipe reset).
        {
            TFakeAgent agent(runtime, /*agentInstanceId=*/1);
            agent.Register();
            agent.PrepareWriteS3(/*cookie=*/1);
            agent.PrepareWriteS3(/*cookie=*/2);
            agent.Disconnect();
        }

        // Agent #2 (same node, new instance) gets a locator, hits S3 SlowDown and gives it back.
        TFakeAgent agent(runtime, /*agentInstanceId=*/2);
        agent.Register();
        const TS3Locator locator = agent.PrepareWriteS3(/*cookie=*/3);
        agent.DiscardWithSlowDown(locator);

        // Throttling is active now: CurrentMaxWritesInFlight == 1 and a short backoff. Let the backoff expire.
        runtime.SimulateSleep(TDuration::Seconds(5));

        // Nothing is really in flight anymore, so the next write must be admitted after the backoff. With leaked slots
        // from agent #1 the tablet believes S3WritesInFlight == 2 >= 1 and never answers.
        agent.SendPrepareWriteS3(/*cookie=*/4);
        const auto admitted = agent.GrabPrepareWriteS3Result(TDuration::Seconds(60));
        UNIT_ASSERT_C(admitted, "TEvPrepareWriteS3 is stuck in PendingPrepareWrites: S3WritesInFlight slots leaked on"
            " agent disconnect");
    }

    Y_UNIT_TEST(SlowDownAloneDoesNotBlockWrites) {
        TTestBasicRuntime runtime;
        StartBlobDepotWithS3(runtime);

        TFakeAgent agent(runtime, /*agentInstanceId=*/1);
        agent.Register();
        const TS3Locator locator = agent.PrepareWriteS3(/*cookie=*/1);
        agent.DiscardWithSlowDown(locator);

        runtime.SimulateSleep(TDuration::Seconds(5));

        agent.SendPrepareWriteS3(/*cookie=*/2);
        UNIT_ASSERT(agent.GrabPrepareWriteS3Result(TDuration::Seconds(60)));
    }

    Y_UNIT_TEST(ParkedRequestOfDisconnectedAgentIsDropped) {
        TTestBasicRuntime runtime(2);
        StartBlobDepotWithS3(runtime);

        // Agent A brings the gate down to CurrentMaxWritesInFlight == 1, lets the backoff expire and takes the only slot.
        TFakeAgent agentA(runtime, /*agentInstanceId=*/1, /*nodeIndex=*/0);
        agentA.Register();
        agentA.DiscardWithSlowDown(agentA.PrepareWriteS3(/*cookie=*/1));
        runtime.SimulateSleep(TDuration::Seconds(5));
        const TS3Locator held = agentA.PrepareWriteS3(/*cookie=*/2);

        // Agent B (another node) parks a request behind the closed gate and dies without holding any slot.
        {
            TFakeAgent agentB(runtime, /*agentInstanceId=*/2, /*nodeIndex=*/1);
            agentB.Register();
            agentB.SendPrepareWriteS3(/*cookie=*/3);
            runtime.SimulateSleep(TDuration::Seconds(1));
            agentB.Disconnect();
        }

        // A frees the slot: the tablet drains the queue. B's stale request must be gone by now, otherwise the tablet
        // trips on a pipe server that no longer exists. Then A's next write must go through.
        agentA.Discard(held, /*slowDown=*/false);
        agentA.SendPrepareWriteS3(/*cookie=*/4);
        UNIT_ASSERT_C(agentA.GrabPrepareWriteS3Result(TDuration::Seconds(60)),
            "parked request of a disconnected agent was not dropped");
    }

    Y_UNIT_TEST(SlotsAreReleasedWhenNewPipeReplacesLiveOne) {
        TTestBasicRuntime runtime;
        StartBlobDepotWithS3(runtime);

        // Old agent instance holds a locator; a new instance on the same node registers over a new pipe while the old
        // pipe is still open, and only then the old pipe goes away.
        TFakeAgent oldAgent(runtime, /*agentInstanceId=*/1);
        oldAgent.Register();
        oldAgent.PrepareWriteS3(/*cookie=*/1);

        TFakeAgent agent(runtime, /*agentInstanceId=*/2);
        agent.Register();
        oldAgent.Disconnect();

        agent.DiscardWithSlowDown(agent.PrepareWriteS3(/*cookie=*/2));
        runtime.SimulateSleep(TDuration::Seconds(5));

        agent.SendPrepareWriteS3(/*cookie=*/3);
        UNIT_ASSERT_C(agent.GrabPrepareWriteS3Result(TDuration::Seconds(60)),
            "slot of the replaced connection leaked");
    }

    Y_UNIT_TEST(DisconnectBetweenPrepareExecuteAndComplete) {
        TTestBasicRuntime runtime;
        TLogWriteHold logWriteHold;
        StartBlobDepotWithS3(runtime, &logWriteHold);

        // Agent #1 sends a prepare; the tablet runs TTxPrepareWriteS3::Execute, but the redo log write is held so
        // Complete does not happen. The agent disconnects in this window, and only then the log write is let through.
        {
            TFakeAgent agent(runtime, /*agentInstanceId=*/1);
            agent.Register();
            logWriteHold.Active = true;
            agent.SendPrepareWriteS3(/*cookie=*/1);
            runtime.SimulateSleep(TDuration::Seconds(1));
            agent.Disconnect();
            logWriteHold.Release(runtime);
            runtime.SimulateSleep(TDuration::Seconds(1));
        }

        // Agent #2 must find the counter consistent: after SlowDown brings the gate to 1 its write still goes through.
        TFakeAgent agent(runtime, /*agentInstanceId=*/2);
        agent.Register();
        agent.DiscardWithSlowDown(agent.PrepareWriteS3(/*cookie=*/2));
        runtime.SimulateSleep(TDuration::Seconds(5));

        agent.SendPrepareWriteS3(/*cookie=*/3);
        UNIT_ASSERT_C(agent.GrabPrepareWriteS3Result(TDuration::Seconds(60)),
            "S3WritesInFlight got out of sync with agent.S3WritesInFlight across disconnect");
    }
}
