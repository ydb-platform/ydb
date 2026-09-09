#include "skeleton_vmovedpatch_actor.h"

#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
Y_UNIT_TEST_SUITE(VMovedPatchBlock82) {
    void RunMovedPatch(bool expired) {
        TTestBasicRuntime runtime;
        TAppPrepare app;
        app.ClearDomainsAndHive();
        runtime.Initialize(app.Unwrap());
        runtime.AdvanceCurrentTime(TDuration::Hours(1));
        const TActorId edge = runtime.AllocateEdgeActor();
        const ui32 group = 1;
        runtime.RegisterService(MakeBlobStorageProxyID(group), edge);
        const auto info = MakeIntrusive<TBlobStorageGroupInfo>(TErasureType::Erasure8Plus2Block);
        const auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        const auto vctx = MakeIntrusive<TVDiskContext>(edge, info->PickTopology(), counters,
            info->GetVDiskId(0), nullptr, NPDisk::DEVICE_TYPE_UNKNOWN);
        const TString data(8193, 'x');
        const TLogoBlobID original(1, 1, 1, 0, data.size(), 0);
        const TLogoBlobID patched(1, 1, 2, 0, data.size(), 0);
        const TInstant now = runtime.GetCurrentTime();
        const TInstant deadline = TInstant::Seconds((now + TDuration::Minutes(1)).Seconds());
        auto request = std::make_unique<TEvBlobStorage::TEvVMovedPatch>(group, group, original,
            patched, info->GetVDiskId(0), false, 42, expired ? TInstant::Seconds(1) : deadline);
        request->AddDiff(7, TString("changed"));
        auto event = TEvBlobStorage::TEvVMovedPatch::TPtr(static_cast<TEventHandle<TEvBlobStorage::TEvVMovedPatch>*>(
            new IEventHandle(edge, edge, request.release())));
        const TActorId actor = runtime.Register(CreateSkeletonVMovedPatchActor(edge,
            TOutOfSpaceStatus(0, 1), event, nullptr, nullptr, 0, vctx));
        TAutoPtr<IEventHandle> handle;
        if (expired) {
            const auto* result = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVMovedPatchResult>(handle);
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus(), NKikimrProto::DEADLINE);
            return;
        }
        const auto* get = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvGet>(handle);
        UNIT_ASSERT_VALUES_EQUAL(get->Deadline, deadline);
        UNIT_ASSERT_VALUES_EQUAL(get->QuerySize, 1);
        UNIT_ASSERT_VALUES_EQUAL(get->Queries[0].Id, original);
        UNIT_ASSERT_VALUES_EQUAL(get->Queries[0].Shift, 0);
        UNIT_ASSERT_VALUES_EQUAL(get->Queries[0].Size, data.size());
        const ui64 getCookie = handle->Cookie;
        auto getResult = std::make_unique<TEvBlobStorage::TEvGetResult>(NKikimrProto::OK, 1, group);
        getResult->Responses[0].Status = NKikimrProto::OK;
        getResult->Responses[0].Id = original;
        getResult->Responses[0].Buffer = TRope(data);
        runtime.Send(new IEventHandle(actor, edge, getResult.release(), 0, getCookie));
        const auto* put = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvPut>(handle);
        TString expected(data);
        expected.replace(7, 7, "changed");
        UNIT_ASSERT_VALUES_EQUAL(put->Deadline, deadline);
        UNIT_ASSERT_VALUES_EQUAL(put->Id, patched);
        UNIT_ASSERT_VALUES_EQUAL(TString(put->Buffer.data(), put->Buffer.size()), expected);
        const ui64 putCookie = handle->Cookie;
        runtime.Send(new IEventHandle(actor, edge, new TEvBlobStorage::TEvPutResult(
            NKikimrProto::OK, patched, TStorageStatusFlags(), group, 1), 0, putCookie));
        const auto* result = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVMovedPatchResult>(handle);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus(), NKikimrProto::OK);
    }

    Y_UNIT_TEST(FiniteDeadlineFullGetAndPut) {
        RunMovedPatch(false);
    }

    Y_UNIT_TEST(ExpiredDeadline) {
        RunMovedPatch(true);
    }
}
}
