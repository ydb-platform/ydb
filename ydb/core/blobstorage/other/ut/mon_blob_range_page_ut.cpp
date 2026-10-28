#include <ydb/core/blobstorage/other/mon_blob_range_page.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace {

struct TRequest : NMonitoring::TMonService2HttpRequest {
    TCgiParameters Params;

    TRequest(ui64 fromTabletId, ui64 toTabletId)
        : TMonService2HttpRequest(nullptr, nullptr, nullptr, nullptr, "", nullptr)
    {
        Params.InsertUnescaped("groupId", "0");
        Params.InsertUnescaped("tabletId", "1");
        Params.InsertUnescaped("from", TLogoBlobID(fromTabletId, 1, 1, 0, 0, 0).ToString());
        Params.InsertUnescaped("to", TLogoBlobID(toTabletId, 1, 2, 0, 0, 0).ToString());
    }

    const TCgiParameters& GetParams() const override {
        return Params;
    }
};

void CheckRange(ui64 fromTabletId, ui64 toTabletId) {
    NActors::TTestActorRuntimeBase runtime;
    runtime.Initialize();
    const auto edge = runtime.AllocateEdgeActor();
    runtime.RegisterService(MakeBlobStorageProxyID(0), edge);
    const auto actor = runtime.Register(CreateMonBlobRangeActor());
    TRequest request(fromTabletId, toTabletId);
    runtime.Send(new IEventHandle(actor, edge, new NMon::TEvHttpInfo(request, 42), 0, 123));

    TAutoPtr<IEventHandle> handle;
    const auto [range, response] = runtime.GrabEdgeEvents<TEvBlobStorage::TEvRange, NMon::TEvHttpInfoRes>(handle);
    if (fromTabletId == 1 && toTabletId == 1) {
        UNIT_ASSERT(range);
        UNIT_ASSERT_VALUES_EQUAL(range->TabletId, 1);
        UNIT_ASSERT_VALUES_EQUAL(range->From.TabletID(), 1);
        UNIT_ASSERT_VALUES_EQUAL(range->To.TabletID(), 1);
    } else {
        UNIT_ASSERT_C(response, "Mismatched tabletId reached BSProxy");
        UNIT_ASSERT_STRING_CONTAINS(response->Answer, "HTTP/1.1 400 Bad Request\r\n");
        UNIT_ASSERT_STRING_CONTAINS(response->Answer, "from and to must match tabletId");
        UNIT_ASSERT_VALUES_EQUAL(response->SubRequestId, 42);
        UNIT_ASSERT_VALUES_EQUAL(handle->Cookie, 123);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TMonBlobRange) {
    Y_UNIT_TEST(RejectMismatchedFrom) { CheckRange(2, 1); }
    Y_UNIT_TEST(RejectMismatchedTo) { CheckRange(1, 2); }
    Y_UNIT_TEST(RejectMismatchedBoth) { CheckRange(2, 2); }
    Y_UNIT_TEST(AcceptMatchingTabletId) { CheckRange(1, 1); }
}

} // namespace NKikimr
