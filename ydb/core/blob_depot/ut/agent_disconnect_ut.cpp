#include "../blob_depot.h"
#include "../events.h"
#include "../s3_router_events.h"

#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/control/lib/immediate_control_board_impl.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NBlobDepot {
namespace {

struct TAgent {
    TActorId Edge;
    TActorId Pipe;
    ui32 NodeIndex;
};

struct TTestEnv {
    static constexpr ui64 TabletId = 72075186224000000;
    TTestBasicRuntime Runtime{3};
    THashSet<ui64> PrepareResults;
    THashMap<TActorId, ui64> RequestIds;
    THashMap<std::pair<TActorId, ui64>, ui64> PrepareCookies;
    TTestActorRuntime::TEventObserverHolder Observer;

    TTestEnv() {
        SetupTabletServices(Runtime, nullptr, true);
        Runtime.GetAppData().Icb->CreateConfigControls(true);
        TControlBoard::SetValue(1, Runtime.GetAppData().Icb->BlobDepotControls.S3MaxWritesInFlight);

        // Only locator allocation is under test. Park external S3 requests in an edge actor;
        // no HTTP server, S3 credentials or successful object uploads are needed.
        Runtime.RegisterService(MakeBlobDepotS3RouterID(TabletId), Runtime.AllocateEdgeActor());
        Observer = Runtime.AddObserver([this](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case NStorage::TEvNodeWardenAcquireBlobDepotS3Router::EventType:
                case NStorage::TEvNodeWardenReleaseBlobDepotS3Router::EventType:
                    ev.Reset();
                    break;
                case TEvBlobDepot::TEvPrepareWriteS3Result::EventType:
                    PrepareResults.insert(PrepareCookies.at(std::make_pair(ev->Recipient, ev->Cookie)));
                    break;
            }
        });

        auto* info = CreateTestTabletInfo(TabletId, TTabletTypes::BlobDepot);
        info->Channels.resize(3); // two system channels and one data channel, matching the config below
        const auto bootstrapper = CreateTestBootstrapper(Runtime, info, CreateBlobDepot);
        Runtime.EnableScheduleForActor(bootstrapper);
        const auto edge = Runtime.AllocateEdgeActor();
        auto config = std::make_unique<TEvBlobDepot::TEvApplyConfig>();
        auto* proto = config->Record.MutableConfig();
        proto->SetName("disconnect-test");
        auto* system = proto->AddChannelProfiles();
        system->SetCount(2);
        system->SetChannelKind(NKikimrBlobDepot::TChannelKind::System);
        proto->AddChannelProfiles()->SetChannelKind(NKikimrBlobDepot::TChannelKind::Data);
        auto* s3 = proto->MutableS3BackendSettings();
        s3->MutableSyncMode();
        s3->MutableSettings()->SetBucket("test-bucket");
        Runtime.SendToPipe(TabletId, edge, config.release(), 0, GetPipeConfigWithRetries());
        UNIT_ASSERT_C(Runtime.GrabEdgeEventRethrow<TEvBlobDepot::TEvApplyConfigResult>(edge, TDuration::Seconds(5)),
            "BlobDepot did not apply the test configuration");
    }

    TAgent Connect(ui32 nodeIndex, ui64 instanceId = 1) {
        const auto edge = Runtime.AllocateEdgeActor(nodeIndex);
        const auto pipe = Runtime.ConnectToPipe(TabletId, edge, nodeIndex, GetPipeConfigWithRetries());
        const TAgent agent{edge, pipe, nodeIndex};
        auto request = std::make_unique<TEvBlobDepot::TEvRegisterAgent>();
        request->Record.SetAgentInstanceId(instanceId);
        Send(agent, request.release());
        UNIT_ASSERT_C(Runtime.GrabEdgeEventRethrow<TEvBlobDepot::TEvRegisterAgentResult>(edge, TDuration::Seconds(1)),
            "BlobDepot did not register the test agent");
        return agent;
    }

    ui64 Send(const TAgent& agent, IEventBase* event) {
        // BlobDepot requires consecutive request cookies starting at one on every pipe.
        const ui64 cookie = ++RequestIds[agent.Pipe];
        Runtime.SendToPipe(agent.Pipe, agent.Edge, event, agent.NodeIndex, cookie);
        return cookie;
    }

    void Disconnect(const TAgent& agent) {
        Runtime.ClosePipe(agent.Pipe, agent.Edge, agent.NodeIndex);
        Runtime.SimulateSleep(TDuration::MilliSeconds(100));
    }

    void Prepare(const TAgent& agent, ui64 cookie) {
        auto request = std::make_unique<TEvBlobDepot::TEvPrepareWriteS3>();
        auto* item = request->Record.AddItems();
        item->SetKey(TStringBuilder() << "key-" << cookie);
        item->SetLen(100);
        const ui64 requestId = Send(agent, request.release());
        PrepareCookies.emplace(std::make_pair(agent.Edge, requestId), cookie);
    }

    void ExpectPending(ui64 cookie) {
        Runtime.SimulateSleep(TDuration::MilliSeconds(100));
        UNIT_ASSERT_C(!PrepareResults.contains(cookie), "write must wait for the occupied S3 slot");
    }

    void ExpectPrepared(const TAgent& agent, ui64 cookie) {
        const auto response = Runtime.GrabEdgeEventRethrow<TEvBlobDepot::TEvPrepareWriteS3Result>(
            agent.Edge, TDuration::Seconds(1));
        UNIT_ASSERT_C(response, "S3 write queue did not resume after agent disconnect/reconnect");
        UNIT_ASSERT_VALUES_EQUAL(PrepareCookies.at(std::make_pair(agent.Edge, response->Cookie)), cookie);
        const auto& record = response->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.ItemsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(record.GetItems(0).GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT(record.GetItems(0).HasS3Locator());
    }

    void QueryBlocks(const TAgent& agent, ui64 tabletId) {
        auto request = std::make_unique<TEvBlobDepot::TEvQueryBlocks>();
        request->Record.AddTabletIds(tabletId);
        Send(agent, request.release());
        const auto response = Runtime.GrabEdgeEventRethrow<TEvBlobDepot::TEvQueryBlocksResult>(
            agent.Edge, TDuration::Seconds(1));
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetBlockedGenerations(0), 0);
    }

    void Block(const TAgent& agent, ui64 tabletId) {
        auto request = std::make_unique<TEvBlobDepot::TEvBlock>();
        request->Record.SetTabletId(tabletId);
        request->Record.SetBlockedGeneration(1);
        request->Record.SetIssuerGuid(123);
        Send(agent, request.release());
    }

    void ExpectBlocked(const TAgent& agent, NKikimrProto::EReplyStatus status = NKikimrProto::OK) {
        const auto response = Runtime.GrabEdgeEventRethrow<TEvBlobDepot::TEvBlockResult>(
            agent.Edge, TDuration::Seconds(2));
        UNIT_ASSERT_C(response, "block did not finish after the disconnected agent timed out");
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetStatus(), status);
    }
};

void CheckReconnectReleasesWrites(ui64 newInstanceId) {
    TTestEnv env;
    const auto oldAgent = env.Connect(0);
    const auto otherAgent = env.Connect(1);
    env.Prepare(oldAgent, 1);
    env.ExpectPrepared(oldAgent, 1);
    // The old connection owns both an allocated locator and a queued request.
    env.Prepare(oldAgent, 2);
    env.Prepare(otherAgent, 3);
    env.ExpectPending(2);
    env.ExpectPending(3);

    // Register the replacement before the old pipe reports disconnection.
    const auto replacement = env.Connect(0, newInstanceId);
    env.ExpectPrepared(otherAgent, 3);
    UNIT_ASSERT(!env.PrepareResults.contains(2));

    // A late disconnect of the superseded pipe must not disconnect the replacement.
    env.Disconnect(oldAgent);
    env.Prepare(replacement, 4);
    env.ExpectPending(4);
    env.Disconnect(otherAgent);
    env.ExpectPrepared(replacement, 4);
}

} // namespace

Y_UNIT_TEST_SUITE(BlobDepotAgentDisconnect) {
    Y_UNIT_TEST(DisconnectReleasesS3WriteSlots) {
        TTestEnv env;
        const auto owner = env.Connect(0);
        const auto waiter = env.Connect(1);
        env.Prepare(owner, 1);
        env.ExpectPrepared(owner, 1);
        env.Prepare(waiter, 2);
        env.ExpectPending(2);
        env.Disconnect(owner);
        env.ExpectPrepared(waiter, 2);
    }

    Y_UNIT_TEST(DisconnectDropsQueuedWritesBeforeReleasingSlots) {
        TTestEnv env;
        const auto owner = env.Connect(0);
        const auto waiter = env.Connect(1);
        env.Prepare(owner, 1);
        env.ExpectPrepared(owner, 1);
        env.Prepare(owner, 2);
        env.Prepare(waiter, 3);
        env.ExpectPending(2);
        env.ExpectPending(3);
        env.Disconnect(owner);
        env.ExpectPrepared(waiter, 3);
        UNIT_ASSERT(!env.PrepareResults.contains(2));
    }

    Y_UNIT_TEST(DisconnectDropsQueuedWritesWithoutAllocatedSlots) {
        TTestEnv env;
        const auto owner = env.Connect(0);
        const auto departed = env.Connect(1);
        const auto waiter = env.Connect(2);
        env.Prepare(owner, 1);
        env.ExpectPrepared(owner, 1);
        env.Prepare(departed, 2);
        env.Prepare(waiter, 3);
        env.ExpectPending(2);
        env.ExpectPending(3);
        env.Disconnect(departed);
        env.ExpectPending(3);
        env.Disconnect(owner);
        env.ExpectPrepared(waiter, 3);
        UNIT_ASSERT(!env.PrepareResults.contains(2));
    }

    Y_UNIT_TEST(ReconnectSameInstanceReleasesS3WriteSlots) {
        CheckReconnectReleasesWrites(1);
    }

    Y_UNIT_TEST(ReconnectNewInstanceReleasesS3WriteSlots) {
        CheckReconnectReleasesWrites(2);
    }

    Y_UNIT_TEST(ReconnectDeliversPendingBlockWithoutInvalidatedSteps) {
        TTestEnv env;
        constexpr ui64 tabletId = 12345;
        const auto lessee = env.Connect(0);
        const auto blocker = env.Connect(1);
        env.QueryBlocks(lessee, tabletId);
        env.Disconnect(lessee);
        env.Block(blocker, tabletId);
        env.Runtime.SimulateSleep(TDuration::MilliSeconds(100));

        // No IDs were allocated, so only BlockToDeliver can trigger the push on reconnect.
        const auto replacement = env.Connect(0);
        const auto push = env.Runtime.GrabEdgeEventRethrow<TEvBlobDepot::TEvPushNotify>(
            replacement.Edge, TDuration::MilliSeconds(100));
        UNIT_ASSERT_C(push, "pending block was not delivered to the reconnected agent");
        UNIT_ASSERT_VALUES_EQUAL(push->Get()->Record.InvalidatedStepsSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(push->Get()->Record.BlockedTabletsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(push->Get()->Record.GetBlockedTablets(0).GetTabletId(), tabletId);
        UNIT_ASSERT_VALUES_EQUAL(push->Get()->Record.GetBlockedTablets(0).GetBlockedGeneration(), 1);
        auto ack = std::make_unique<TEvBlobDepot::TEvPushNotifyResult>();
        ack->Record.SetId(push->Cookie);
        env.Send(replacement, ack.release());
        env.ExpectBlocked(blocker);
    }

    Y_UNIT_TEST(BlockCanBeReissuedAfterDisconnectedAgentTimeout) {
        TTestEnv env;
        constexpr ui64 tabletId = 12345;
        const auto lessee = env.Connect(0);
        const auto blocker = env.Connect(1);
        env.QueryBlocks(lessee, tabletId);
        env.Disconnect(lessee);

        // The block finishes via storage while the disconnected agent's lease is still valid.
        // Reissuing the same generation/issuer must not collide with a stale BlockToDeliver.
        env.Block(blocker, tabletId);
        env.ExpectBlocked(blocker);
        env.Block(blocker, tabletId);
        // The mock storage reports that the first request already installed this block.
        env.ExpectBlocked(blocker, NKikimrProto::ALREADY);
    }
}

} // namespace NKikimr::NBlobDepot
