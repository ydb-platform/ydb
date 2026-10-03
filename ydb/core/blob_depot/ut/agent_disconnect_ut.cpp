#include "../blob_depot.h"
#include "../events.h"
#include "../s3_router_events.h"

#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/control/lib/immediate_control_board_impl.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
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
    THashMap<TActorId, ui64> RequestIds;
    TTestActorRuntime::TEventObserverHolder Observer;

    TTestEnv() {
        SetupTabletServices(Runtime, nullptr, true);
        Runtime.GetAppData().Icb->CreateConfigControls(true);

        Runtime.RegisterService(MakeBlobDepotS3RouterID(TabletId), Runtime.AllocateEdgeActor());
        Observer = Runtime.AddObserver([](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case NStorage::TEvNodeWardenAcquireBlobDepotS3Router::EventType:
                case NStorage::TEvNodeWardenReleaseBlobDepotS3Router::EventType:
                    ev.Reset();
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
        UNIT_ASSERT_C(Runtime.GrabEdgeEventRethrow<TEvBlobDepot::TEvApplyConfigResult>(edge, TDuration::Seconds(5)), "BlobDepot did not apply the test configuration");
    }

    TAgent Connect(ui32 nodeIndex) {
        const auto edge = Runtime.AllocateEdgeActor(nodeIndex);
        const auto pipe = Runtime.ConnectToPipe(TabletId, edge, nodeIndex, GetPipeConfigWithRetries());
        const TAgent agent{edge, pipe, nodeIndex};
        auto request = std::make_unique<TEvBlobDepot::TEvRegisterAgent>();
        request->Record.SetAgentInstanceId(1);
        Send(agent, request.release());
        UNIT_ASSERT_C(Runtime.GrabEdgeEventRethrow<TEvBlobDepot::TEvRegisterAgentResult>(edge, TDuration::Seconds(1)), "BlobDepot did not register the test agent");
        return agent;
    }

    ui64 Send(const TAgent& agent, IEventBase* event) {
        const ui64 cookie = ++RequestIds[agent.Pipe];
        Runtime.SendToPipe(agent.Pipe, agent.Edge, event, agent.NodeIndex, cookie);
        return cookie;
    }

    void SendQueryBlocks(const TAgent& agent, ui64 tabletId) {
        auto request = std::make_unique<TEvBlobDepot::TEvQueryBlocks>();
        request->Record.AddTabletIds(tabletId);
        Send(agent, request.release());
    }

    void ExpectQueryBlocks(const TAgent& agent, ui32 expectedGeneration = 0) {
        const auto response = Runtime.GrabEdgeEventRethrow<TEvBlobDepot::TEvQueryBlocksResult>(
            agent.Edge, TDuration::Seconds(1));
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.BlockedGenerationsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetBlockedGenerations(0), expectedGeneration);
    }

    void QueryBlocks(const TAgent& agent, ui64 tabletId) {
        SendQueryBlocks(agent, tabletId);
        ExpectQueryBlocks(agent);
    }

    void Block(const TAgent& agent, ui64 tabletId) {
        auto request = std::make_unique<TEvBlobDepot::TEvBlock>();
        request->Record.SetTabletId(tabletId);
        request->Record.SetBlockedGeneration(1);
        request->Record.SetIssuerGuid(123);
        Send(agent, request.release());
    }
};

void CheckQueryBlocksCommit(bool reboot) {
    TTestEnv env;
    constexpr ui64 tabletId = 12345;
    const auto lessee = env.Connect(0);
    const auto blocker = env.Connect(1);
    env.QueryBlocks(lessee, tabletId);

    ui32 observedGeneration = 0;
    ui32 responses = 0;
    auto queryObserver = env.Runtime.AddObserver<TEvBlobDepot::TEvQueryBlocksResult>(
        [&](TEvBlobDepot::TEvQueryBlocksResult::TPtr& ev) {
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.BlockedGenerationsSize(), 1);
            observedGeneration = ev->Get()->Record.GetBlockedGenerations(0);
            ++responses;
        });
    TBlockEvents<TEvTablet::TEvCommit> commits(env.Runtime, [](const auto& ev) {
        return ev->Get()->TabletID == TTestEnv::TabletId;
    });

    env.Block(blocker, tabletId);
    env.Runtime.WaitFor("block commit", [&] { return !commits.empty(); }, TDuration::Seconds(1));
    env.SendQueryBlocks(lessee, tabletId);
    env.Runtime.SimulateSleep(TDuration::MilliSeconds(100));

    if (reboot) {
        const ui32 generationBeforeReboot = observedGeneration;
        commits.Stop();
        commits.clear();
        RebootTablet(env.Runtime, TTestEnv::TabletId, env.Runtime.AllocateEdgeActor());
        const auto reconnected = env.Connect(0);
        env.QueryBlocks(reconnected, tabletId);

        UNIT_ASSERT_C(generationBeforeReboot <= observedGeneration,
                      "Blocked generation decreased after BlobDepot reboot: "
                      << generationBeforeReboot << " -> " << observedGeneration);
    } else {
        UNIT_ASSERT_VALUES_EQUAL_C(responses, 0, "QueryBlocks replied before the block was committed");
        commits.Stop().Unblock();
        env.ExpectQueryBlocks(lessee, 1);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(BlobDepotAgentDisconnect) {
    Y_UNIT_TEST(QueryBlocksRemainMonotonicAfterReboot) {
        CheckQueryBlocksCommit(true);
    }

    Y_UNIT_TEST(QueryBlocksWaitForCommit) {
        CheckQueryBlocksCommit(false);
    }
}

} // namespace NKikimr::NBlobDepot
