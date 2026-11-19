#include "dsproxy_env_mock_ut.h"
#include "dsproxy_test_state_ut.h"

namespace NKikimr {
namespace {

void CheckRangePagination(bool reverse, ui32 crcMode) {
    TTestBasicRuntime runtime;
    SetupRuntime(runtime);
    TDSProxyEnv env;
    env.Configure(runtime, TBlobStorageGroupType(TErasureType::ErasureMirror3dc),
        0, 0, TBlobStorageGroupInfo::EEM_NONE);
    TTestState state(runtime, env.Info);
    auto& group = state.GetGroupMock();
    constexpr ui64 tabletId = 72075186224047637;
    constexpr ui32 blobCount = 8097; // one more than the Range actor's page size
    const TString data = "data";
    TVector<TLogoBlobID> ids;
    for (ui32 cookie = 1; cookie <= blobCount; ++cookie) {
        const auto id = TLogoBlobID::Make(tabletId, 1, 1, 0, data.size(), cookie, crcMode);
        ids.push_back(id);
        // Fully replicated data, including handoffs: all disks have the same
        // page boundary. Use the real erasure splitter, including its CRC.
        for (ui32 slot = 0; slot < 9; ++slot) {
            group.PutPartAtSlot(id, slot, slot % 3 + 1, data);
        }
    }
    auto query = std::make_unique<TEvBlobStorage::TEvRange>(tabletId,
        reverse ? ids.back() : ids.front(), reverse ? ids.front() : ids.back(),
        false, TInstant::Max(), true);
    runtime.Send(new IEventHandle(env.RealProxyActorId, state.EdgeActor, query.release()), 0, true);

    ui32 fullPages = 0;
    for (;;) {
        TAutoPtr<IEventHandle> handle;
        auto [get, result] = runtime.GrabEdgeEventsRethrow<TEvBlobStorage::TEvVGet,
            TEvBlobStorage::TEvRangeResult>(handle);
        if (get) {
            UNIT_ASSERT(get->Record.HasRangeQuery());
            TEvBlobStorage::TEvVGet::TPtr request(
                static_cast<TEventHandle<TEvBlobStorage::TEvVGet>*>(handle.Release()));
            auto reply = state.CreateEventResultPtr(request, NKikimrProto::UNKNOWN);
            group.OnVGet(*request->Get(), *reply->Get());
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(), NKikimrProto::OK);
            fullPages += reply->Get()->Record.GetIsRangeOverflow();
            runtime.Send(reply.Release());
        } else {
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(fullPages, 9);
            UNIT_ASSERT_VALUES_EQUAL(result->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(result->Responses.size(), ids.size());
            for (ui32 i = 0; i < ids.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(result->Responses[i].Id,
                    ids[reverse ? ids.size() - 1 - i : i]);
            }
            break;
        }
    }
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TDSProxyRangePagination) {
    Y_UNIT_TEST(LeavingRealmsCanHoldTheEntireConfirmedBlob) {
        using DC = NKikimrBlobStorage::TDataCenterState;
        TTestBasicRuntime runtime;
        SetupRuntime(runtime);
        TDSProxyEnv env;
        env.Configure(runtime, TBlobStorageGroupType(TErasureType::ErasureMirror3dc), 0, 0,
            TBlobStorageGroupInfo::EEM_NONE, DormantTimeoutDefaultControl, false,
            {DC::INCLUDED, DC::LEAVING, DC::LEAVING}, NKikimrBlobStorage::TGroupState::DC_SOLO_DISASTER);
        TTestState state(runtime, env.Info);
        TGroupMock& group = state.GetGroupMock();
        const TLogoBlobID id(123456, 1, 1, 0, state.BlobSize, 0);
        for (ui32 slot = 0; slot < 6; ++slot) {
            auto& disk = group.GetVDiskInSubgroup(id, slot);
            if (disk.GetVDiskId().FailRealm != 0) {
                group.PutPartAtSlot(id, slot, slot % 3 + 1, state.BlobData);
            }
        }
        for (bool leavingUnavailable : {false, true}) {
            auto query = std::make_unique<TEvBlobStorage::TEvRange>(id.TabletID(), id, id, false, TInstant::Max(), true);
            runtime.Send(new IEventHandle(env.RealProxyActorId, state.EdgeActor, query.release()), 0);
            for (ui32 i = 0; i < 9; ++i) {
                auto request = state.GrabEventPtr<TEvBlobStorage::TEvVGet>();
                auto reply = state.CreateEventResultPtr(request, NKikimrProto::UNKNOWN);
                const TVDiskID disk = VDiskIDFromVDiskID(request->Get()->Record.GetVDiskID());
                if (leavingUnavailable && disk.FailRealm != 0) {
                    reply->Get()->Record.SetStatus(NKikimrProto::ERROR);
                } else {
                    group.OnVGet(*request->Get(), *reply->Get());
                }
                runtime.Send(reply.Release());
            }
            auto result = state.GrabEventPtr<TEvBlobStorage::TEvRangeResult>();
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, leavingUnavailable ? NKikimrProto::ERROR : NKikimrProto::OK);
            if (!leavingUnavailable) {
                UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses.front().Id, id);
            }
        }
    }
    Y_UNIT_TEST(ReverseWithCrc) {
        CheckRangePagination(true, TErasureType::CrcModeWholePart);
    }
    Y_UNIT_TEST(ForwardWithCrc) {
        CheckRangePagination(false, TErasureType::CrcModeWholePart);
    }
    Y_UNIT_TEST(ReverseWithoutCrc) {
        CheckRangePagination(true, TErasureType::CrcModeNone);
    }
}

} // namespace NKikimr
