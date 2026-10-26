#include "../blob_depot.h"
#include "../agent/agent.h"
#include "../events.h"

#include <ydb/core/base/tablet.h>
#include <ydb/core/control/lib/immediate_control_board_impl.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NBlobDepot {
namespace {

struct TTestEnv {
    static constexpr ui64 TabletId = 72075186224000000;
    static constexpr ui32 VirtualGroupId = 0x80000000;
    TTestBasicRuntime Runtime{3};

    TTestEnv() {
        SetupTabletServices(Runtime, nullptr, true);
        Runtime.GetAppData().Icb->CreateConfigControls(true);

        auto* info = CreateTestTabletInfo(TabletId, TTabletTypes::BlobDepot);
        info->Channels.resize(3); // Two system channels and one data channel.
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
        proto->SetVirtualGroupId(VirtualGroupId);
        Runtime.SendToPipe(TabletId, edge, config.release(), 0, GetPipeConfigWithRetries());
        UNIT_ASSERT_C(Runtime.GrabEdgeEventRethrow<TEvBlobDepot::TEvApplyConfigResult>(edge, TDuration::Seconds(5)),
            "BlobDepot did not apply the test configuration");
    }
};

void CheckGetCompletesAfterDisconnect(bool nodata) {
    TTestEnv env;
    auto info = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::ErasureNone);
    info->BlobDepotId = TTestEnv::TabletId;
    const auto agentId = env.Runtime.Register(CreateBlobDepotAgent(TTestEnv::VirtualGroupId, info, {}));
    env.Runtime.EnableScheduleForActor(agentId);
    const auto edge = env.Runtime.AllocateEdgeActor();

    TActorId pipeId;
    auto pipeObserver = env.Runtime.AddObserver<TEvTabletPipe::TEvClientConnected>(
        [&](TEvTabletPipe::TEvClientConnected::TPtr& ev) {
            if (ev->Recipient == agentId) {
                pipeId = ev->Get()->ClientId;
            }
        });
    const TString data = "test-data";
    const TLogoBlobID id(12345, 1, 1, 0, data.size(), 0);
    env.Runtime.SendAsync(new IEventHandle(agentId, edge, new TEvBlobStorage::TEvPut(id, data, TInstant::Max())));
    const auto putResult = env.Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvPutResult>(edge, TDuration::Seconds(1));
    UNIT_ASSERT(putResult);
    UNIT_ASSERT_VALUES_EQUAL_C(putResult->Get()->Status, NKikimrProto::OK, putResult->Get()->ErrorReason);
    UNIT_ASSERT(pipeId);

    // Hold the physical read issued by the agent to the storage proxy.
    TBlockEvents<TEvBlobStorage::TEvGetResult> reads(env.Runtime, [&](const auto& ev) {
        return ev->Recipient == agentId;
    });
    env.Runtime.SendAsync(new IEventHandle(agentId, edge, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
        NKikimrBlobStorage::EGetHandleClass::FastRead)));
    env.Runtime.WaitFor("physical read", [&] { return !reads.empty(); }, TDuration::Seconds(1));

    if (nodata) {
        // NODATA makes the agent resolve the key again; disconnect while this resolve is in flight.
        for (auto& ev : reads) {
            auto& msg = *ev->Get();
            for (ui32 i = 0; i < msg.ResponseSz; ++i) {
                msg.Responses[i].Status = NKikimrProto::NODATA;
                msg.Responses[i].Buffer = {};
            }
        }
        TBlockEvents<TEvBlobDepot::TEvResolve> resolves(env.Runtime, [&](const auto& ev) {
            return ev->Sender == agentId;
        });
        reads.Stop().Unblock();
        env.Runtime.WaitFor("resolve after NODATA", [&] { return !resolves.empty(); }, TDuration::Seconds(1));
        env.Runtime.ClosePipe(pipeId, agentId, 0);
    } else {
        // The physical read is not a tablet request: it survives the disconnect and completes normally.
        env.Runtime.ClosePipe(pipeId, agentId, 0);
        env.Runtime.SimulateSleep(TDuration::MilliSeconds(100));
        reads.Stop().Unblock();
    }

    const auto result = env.Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvGetResult>(edge, TDuration::Seconds(1));
    UNIT_ASSERT_C(result, "Get did not complete after BlobDepot tablet disconnect");
    const auto& msg = *result->Get();
    UNIT_ASSERT_VALUES_EQUAL(msg.ResponseSz, 1);
    if (nodata) {
        UNIT_ASSERT_VALUES_EQUAL(msg.Responses[0].Status, NKikimrProto::ERROR);
        UNIT_ASSERT_STRING_CONTAINS(msg.ErrorReason, "disconnected");
    } else {
        UNIT_ASSERT_VALUES_EQUAL_C(msg.Responses[0].Status, NKikimrProto::OK, msg.ErrorReason);
        UNIT_ASSERT_VALUES_EQUAL(msg.Responses[0].Buffer.ConvertToString(), data);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(BlobDepotAgentDisconnect) {
    Y_UNIT_TEST(GetPhysicalReadCompletesAfterDisconnect) {
        CheckGetCompletesAfterDisconnect(false);
    }

    Y_UNIT_TEST(GetNodataResolveFailsOnDisconnect) {
        CheckGetCompletesAfterDisconnect(true);
    }
}

} // namespace NKikimr::NBlobDepot
