#include "dsproxy_env_mock_ut.h"
#include "dsproxy_test_state_ut.h"

#include <ydb/core/blobstorage/base/blobstorage_events.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TDSProxyStatus) {
    Y_UNIT_TEST(SpaceForNewWritesAndStartupQuorum) {
        using G = NKikimrBlobStorage::TGroupState;
        using DC = NKikimrBlobStorage::TDataCenterState;
        auto check = [](const TMirror3dcGroupState& config, unsigned healthyIncludedDisksPerRealm,
                NKikimrProto::EReplyStatus expected, float expectedSpace) {
            TTestBasicRuntime runtime;
            SetupRuntime(runtime);
            TDSProxyEnv env;
            env.Configure(runtime, TBlobStorageGroupType(TErasureType::ErasureMirror3dc), 0, 0,
                TBlobStorageGroupInfo::EEM_NONE, DormantTimeoutDefaultControl, false,
                {config.FailRealmStates.begin(), config.FailRealmStates.end()}, config.State);
            TTestState state(runtime, env.Info);
            runtime.Send(new IEventHandle(env.RealProxyActorId, state.EdgeActor, new TEvBlobStorage::TEvStatus(TInstant::Max())), 0);
            const auto requested = 3 * std::count_if(config.FailRealmStates.begin(), config.FailRealmStates.end(),
                [](auto role) { return !IsExcluded(role); });
            for (auto i = 0; i < requested; ++i) {
                auto request = state.GrabEventPtr<TEvBlobStorage::TEvVStatus>();
                TVDiskID disk = VDiskIDFromVDiskID(request->Get()->Record.GetVDiskID());
                auto role = config.FailRealmStates[disk.FailRealm];
                UNIT_ASSERT(!IsExcluded(role));
                bool healthy = !IsIncluded(role) || disk.FailDomain < healthyIncludedDisksPerRealm;
                auto status = healthy ? NKikimrProto::OK : NKikimrProto::ERROR;
                auto reply = std::make_unique<TEvBlobStorage::TEvVStatusResult>(status, disk, true, /*replicated=*/false, false, 1);
                reply->Record.SetApproximateFreeSpaceShare(IsIncluded(role) ? 0.5f : 0.125f);
                runtime.Send(new IEventHandle(request->Sender, request->Recipient, reply.release(), 0, request->Cookie), 0);
            }
            auto result = state.GrabEventPtr<TEvBlobStorage::TEvStatusResult>();
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, expected);
            if (expected == NKikimrProto::OK) {
                UNIT_ASSERT_VALUES_EQUAL(result->Get()->ApproximateFreeSpaceShare, expectedSpace);
            }
        };
        check({G::DC_FAILURE, {DC::LEAVING, DC::INCLUDED, DC::INCLUDED}}, 3, NKikimrProto::OK, 0.5f);
        check({G::DC_SOLO_RECOVERY_INV1_PENDING, {DC::INCLUDED, DC::RECOVERING, DC::EXCLUDED}}, 3, NKikimrProto::OK, 0.5f);
        check({G::DC_SOLO_RECOVERY_INV1, {DC::INCLUDED, DC::RECOVERING, DC::EXCLUDED}}, 3, NKikimrProto::OK, 0.125f);
        check({G::DC_SOLO_DISASTER, {DC::LEAVING, DC::INCLUDED, DC::LEAVING}}, 2, NKikimrProto::OK, 0.0f);
        check({G::DC_SOLO_DISASTER, {DC::LEAVING, DC::INCLUDED, DC::LEAVING}}, 1, NKikimrProto::ERROR, 0.0f);
    }
}

} // namespace NKikimr
