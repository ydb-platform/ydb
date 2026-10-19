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
