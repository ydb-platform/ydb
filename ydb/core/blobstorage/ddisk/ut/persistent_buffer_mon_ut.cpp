#include <ydb/core/blobstorage/ddisk/persistent_buffer_mon.h>
#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/monlib/service/mon_service_http_request.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/stream/null.h>

namespace NKikimr {
namespace {

class TFakeMonHttpRequest: public NMonitoring::IMonHttpRequest {
public:
    TFakeMonHttpRequest(HTTP_METHOD method, TString uri, THttpHeaders headers, TString body)
        : Method(method)
        , Uri(std::move(uri))
        , Headers(std::move(headers))
        , Body(std::move(body))
        , Params(TStringBuf(Uri).After('?'))
        , PostParams(Body)
    {
    }

    IOutputStream& Output() override {
        return Cnull;
    }

    HTTP_METHOD GetMethod() const override {
        return Method;
    }

    TStringBuf GetPath() const override {
        return TStringBuf(Uri).Before('?');
    }

    TStringBuf GetPathInfo() const override {
        return GetPath();
    }

    TStringBuf GetUri() const override {
        return Uri;
    }

    const TCgiParameters& GetParams() const override {
        return Params;
    }

    const TCgiParameters& GetPostParams() const override {
        return PostParams;
    }

    TStringBuf GetPostContent() const override {
        return Body;
    }

    const THttpHeaders& GetHeaders() const override {
        return Headers;
    }

    TStringBuf GetHeader(TStringBuf name) const override {
        if (const auto* header = Headers.FindHeader(name)) {
            return header->Value();
        }
        return {};
    }

    TStringBuf GetCookie(TStringBuf) const override {
        return {};
    }

    TString GetRemoteAddr() const override {
        return {};
    }

    TString GetServiceTitle() const override {
        return {};
    }

    NMonitoring::IMonPage* GetPage() const override {
        return nullptr;
    }

    NMonitoring::IMonHttpRequest* MakeChild(NMonitoring::IMonPage*, const TString&) const override {
        return nullptr;
    }

private:
    const HTTP_METHOD Method;
    const TString Uri;
    const THttpHeaders Headers;
    const TString Body;
    const TCgiParameters Params;
    const TCgiParameters PostParams;
};

struct TMonTest {
    NActors::TTestActorRuntime Runtime;
    NActors::TActorId Mon;
    NActors::TActorId Edge;
    NActors::TActorId Warden;
    NActors::TActorId PB;
    NActors::TActorId OtherPB;
    NActors::TActorId PBEdge;
    std::unique_ptr<TFakeMonHttpRequest> HttpRequest;

    TMonTest() {
        Runtime.Initialize({new TAppData(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr),
            nullptr, nullptr, {}, {}});
        Edge = Runtime.AllocateEdgeActor();
        Warden = Runtime.AllocateEdgeActor();
        PBEdge = Runtime.AllocateEdgeActor();
        PB = MakeBlobStoragePersistentBufferId(Runtime.GetNodeId(), 1, 1);
        OtherPB = MakeBlobStoragePersistentBufferId(Runtime.GetNodeId(), 2, 1);
        Runtime.RegisterService(MakeBlobStorageNodeWardenID(Runtime.GetNodeId()), Warden);
        Runtime.RegisterService(PB, PBEdge);
        Runtime.RegisterService(OtherPB, PBEdge);
        NKikimrConfig::TAppConfig config;
        TAppData appData(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr);
        Mon = Runtime.Register(CreateMonPersistentBufferActor(config, appData));
    }

    void Request(const TString& params) {
        HttpRequest = std::make_unique<TFakeMonHttpRequest>(HTTP_METHOD_GET,
            "/actors/persistent_buffer?" + params, THttpHeaders(), TString());
        Runtime.Send(new NActors::IEventHandle(Mon, Edge, new NActors::NMon::TEvHttpInfo(*HttpRequest)), 0, true);
    }

    void ListBuffers() {
        auto request = Runtime.GrabEdgeEventRethrow<TEvNodeWardenListLocalDDisks>(Warden);
        auto reply = std::make_unique<TEvNodeWardenListLocalDDisksResult>();
        reply->Infos.push_back({{}, PB});
        reply->Infos.push_back({{}, OtherPB});
        Runtime.Send(new NActors::IEventHandle(Mon, Warden, reply.release(), 0, request->Cookie));
    }

    TString Response() {
        auto response = Runtime.GrabEdgeEventRethrow<NActors::NMon::TEvHttpInfoRes>(Edge);
        TStringStream out;
        response->Get()->Output(out);
        return out.Str();
    }

    TString PBParam() const {
        TCgiParameters params;
        params.InsertUnescaped("pb", ToString(PB));
        return params.Print();
    }
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(TPersistentBufferMonTest) {
    Y_UNIT_TEST(SummaryDoesNotRequestTablets) {
        TMonTest test;
        test.Request(test.PBParam() + "&showTablets=1&tabletOpen." + ToString(test.PB) + "=1");
        test.ListBuffers();
        auto request = test.Runtime.GrabEdgeEventRethrow<NDDisk::TEvGetPersistentBufferInfo>(test.PBEdge);
        UNIT_ASSERT(!request->Get()->DescribeTablets);
        auto reply = std::make_unique<NDDisk::TEvPersistentBufferInfo>();
        reply->StartedAt = TInstant::Now();
        reply->SectorSize = 4096;
        reply->ChunkSize = 4096;
        reply->AllocatedChunks = reply->MaxChunks = reply->FreeSectors = 0;
        reply->InMemoryCacheSize = reply->InMemoryCacheLimit = 0;
        reply->PendingEvents = reply->DiskOperationsInflight = 0;
        test.Runtime.Send(new NActors::IEventHandle(test.Mon, test.PBEdge, reply.release(), 0, request->Cookie));
        const auto response = test.Response();
        UNIT_ASSERT_STRING_CONTAINS(response, "aria-expanded=\"false\">Show tablets");
        UNIT_ASSERT_STRING_CONTAINS(response, "pb-mon-tablets-content\" hidden");
        UNIT_ASSERT(!response.Contains("id=\"pb-mon-showTablets\""));
        // A completed request's scheduled timeout must be harmless.
        test.Runtime.Send(new NActors::IEventHandle(test.Mon, test.Edge, new NActors::TEvents::TEvWakeup(1)));
        test.Request("action=tablets&" + test.PBParam() + "&page=-1");
        UNIT_ASSERT_STRING_CONTAINS(test.Response(), "400 Bad Request");
    }

    Y_UNIT_TEST(TabletsApiRequestsOneBufferAndOnePage) {
        TMonTest test;
        test.Request("action=tablets&" + test.PBParam() + "&page=2");
        test.ListBuffers();
        auto request = test.Runtime.GrabEdgeEventRethrow<NDDisk::TEvGetPersistentBufferInfo>(test.PBEdge);
        UNIT_ASSERT_VALUES_EQUAL(request->Recipient, test.PB);
        UNIT_ASSERT(request->Get()->DescribeTablets);
        UNIT_ASSERT(!request->Get()->DescribeFreeSpace);
        UNIT_ASSERT_VALUES_EQUAL(request->Get()->TabletsLimit, 100);
        UNIT_ASSERT_VALUES_EQUAL(request->Get()->TabletsOffset, 200);
        auto reply = std::make_unique<NDDisk::TEvPersistentBufferInfo>();
        reply->TabletsTotal = 201;
        reply->TabletsOffset = 200;
        reply->PerTabletStorageLimit = 4096;
        reply->TabletInfos.emplace_back(Max<ui64>(), 1, 3, 4, TInstant::Now(), TInstant::Now(), 2, 1024, 0, 0);
        reply->EraseBarriers[{Max<ui64>(), 0}] = 2;
        test.Runtime.Send(new NActors::IEventHandle(test.Mon, test.PBEdge, reply.release(), 0, request->Cookie));
        const auto response = test.Response();
        UNIT_ASSERT_STRING_CONTAINS(response, "200 OK");
        UNIT_ASSERT_STRING_CONTAINS(response, "Content-Type: application/json");
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(TStringBuf(response).SubStr(response.find("\r\n\r\n") + 4), &json));
        UNIT_ASSERT_VALUES_EQUAL(json["page"].GetUInteger(), 2);
        UNIT_ASSERT_VALUES_EQUAL(json["pages"].GetUInteger(), 3);
        UNIT_ASSERT_VALUES_EQUAL(json["total"].GetString(), "201");
        UNIT_ASSERT_VALUES_EQUAL(json["tablets"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["tablets"][0]["tabletId"].GetString(), ToString(Max<ui64>()));
        UNIT_ASSERT_VALUES_EQUAL(json["tablets"][0]["barrier"].GetString(), "2");
    }

    Y_UNIT_TEST(TabletsApiRejectsInvalidRequests) {
        for (const TString& params : {TString("action=tablets"), TString("action=tablets&pb=one&pb=two"),
                TString("action=tablets&pb=one&page=-1"), TString("action=tablets&pb=one&page=18446744073709551615"),
                TString("action=tablets&pb=one&page=abc")}) {
            TMonTest test;
            test.Request(params);
            const auto response = test.Response();
            UNIT_ASSERT_STRING_CONTAINS(response, "400 Bad Request");
            UNIT_ASSERT_STRING_CONTAINS(response, "application/json");
        }
    }

    Y_UNIT_TEST(TabletsApiMissingBuffer) {
        TMonTest test;
        test.Request("action=tablets&pb=missing");
        test.ListBuffers();
        UNIT_ASSERT_STRING_CONTAINS(test.Response(), "404 Not Found");
    }

    Y_UNIT_TEST(TabletsApiTimeout) {
        TMonTest test;
        test.Request("action=tablets&" + test.PBParam());
        test.ListBuffers();
        auto request = test.Runtime.GrabEdgeEventRethrow<NDDisk::TEvGetPersistentBufferInfo>(test.PBEdge);
        test.Runtime.Send(new NActors::IEventHandle(test.Mon, test.Edge, new NActors::TEvents::TEvWakeup(1)));
        UNIT_ASSERT_STRING_CONTAINS(test.Response(), "504 Gateway Timeout");
        // Ignore a late buffer reply without recreating an inflight request.
        test.Runtime.Send(new NActors::IEventHandle(test.Mon, test.PBEdge,
            new NDDisk::TEvPersistentBufferInfo(), 0, request->Cookie));
        test.Request("action=tablets");
        UNIT_ASSERT_STRING_CONTAINS(test.Response(), "400 Bad Request");
    }
}
} // NKikimr
