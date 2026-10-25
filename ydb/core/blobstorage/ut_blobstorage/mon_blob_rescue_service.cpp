#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/other/mon_blob_rescue_service.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_ut_http_request.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;

namespace {

    struct TRequest : THttpRequestMock {
        HTTP_METHOD Method = HTTP_METHOD_GET;

        HTTP_METHOD GetMethod() const override { return Method; }
    };

} // anonymous namespace

Y_UNIT_TEST_SUITE(BlobRescueService) {
    Y_UNIT_TEST(Page) {
        TTestActorSystem runtime(1);
        runtime.Start();
        const auto actorId = runtime.Register(CreateMonBlobRescueServiceActor(), 1);
        auto http = [&](TRequest& request) {
            const auto edge = runtime.AllocateEdgeActor(1, __FILE__, __LINE__);
            NMonitoring::TMonService2HttpRequest monRequest(nullptr, &request, nullptr, nullptr, "", nullptr);
            runtime.Send(new IEventHandle(actorId, edge, new NMon::TEvHttpInfo(monRequest, 123), 0, 456), 1);
            auto result = runtime.WaitForEdgeActorEvent<NMon::TEvHttpInfoRes>(edge);
            UNIT_ASSERT_VALUES_EQUAL(result->Cookie, 456);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->SubRequestId, 123);
            return result->Get()->Answer;
        };
        bool restoreSeen = false;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            restoreSeen |= ev->GetTypeRewrite() == TEvBlobStorage::EvVRestorePart
                || ev->GetTypeRewrite() == NSysView::TEvSysView::EvGetVSlotsRequest;
            return true;
        };
        TRequest request;
        request.Path = "/blob_rescue_service";
        request.Method = HTTP_METHOD_GET;
        request.CgiParameters.InsertUnescaped("requests", "[80000000:1:0:0:0] [1:1:1:0:0:1024:0]");
        const auto html = http(request);
        UNIT_ASSERT_STRING_CONTAINS(html, "Blob Rescue Service");
        UNIT_ASSERT_STRING_CONTAINS(html, "panel panel-info");
        UNIT_ASSERT_STRING_CONTAINS(html, "<form method=\"post\" action=\"/restore_part\"");
        UNIT_ASSERT_STRING_CONTAINS(html, "<textarea");
        UNIT_ASSERT_STRING_CONTAINS(html, "name=\"inflight\"");
        UNIT_ASSERT_STRING_CONTAINS(html, "min=\"1\" value=\"1\" required");
        UNIT_ASSERT_STRING_CONTAINS(html, "name=\"timeout\" type=\"number\" min=\"1\" value=\"30\" required");
        UNIT_ASSERT(!restoreSeen);
        request.Method = HTTP_METHOD_POST;
        UNIT_ASSERT_STRING_CONTAINS(http(request), "405 Method Not Allowed\r\nAllow: GET\r\n");
        UNIT_ASSERT(!restoreSeen);
        runtime.FilterFunction = nullptr;
    }
}
