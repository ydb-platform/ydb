#include <ydb/core/base/counters.h>

#include <ydb/core/mind/address_classification/counters.h>

#include <ydb/core/mind/address_classification/net_classifier.h>

#include <ydb/core/testlib/test_client.h>

#include <ydb/library/actors/http/http_proxy.cpp>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/file.h>
#include <util/system/tempfile.h>

namespace NKikimr::NNetClassifierTests {

using namespace NNetClassifier;
using namespace Tests;

static THolder<TTempFileHandle> CreateNetDataFile(const TString& content) {
    auto netDataFile = MakeHolder<TTempFileHandle>();

    netDataFile->Write(content.Data(), content.Size());
    netDataFile->FlushData();

    return netDataFile;
}

static void Wait() {
    Sleep(TDuration::MilliSeconds(333));
}

static TString FormNetData() {
    return "10.99.99.224/27\tSAS\n"
           "185.32.186.0/29\tIVA\n"
           "185.32.186.16/29\tMYT\n"
           "185.32.186.24/31\tIVA\n"
           "185.32.186.26/31\tIVA\n"
           "37.29.21.132/30\tMYT\n"
           "83.220.51.8/30\tMYT\n"
           "2a02:6b8:f030:3000::/52\tMYT\n"
           "2a02:6b8:fc00::/48\tSAS";
}

static TAutoPtr<IEventHandle> GetClassifierUpdate(TServer& server, const TActorId sender) {
    auto& actorSystem = *server.GetRuntime();

    actorSystem.Send(
        new IEventHandle(MakeNetClassifierID(), sender,
            new TEvNetClassifier::TEvSubscribe()
    ));

    TAutoPtr<IEventHandle> handle;
    actorSystem.GrabEdgeEvent<TEvNetClassifier::TEvClassifierUpdate>(handle);

    UNIT_ASSERT(handle);
    UNIT_ASSERT_VALUES_EQUAL(handle->Recipient, sender);

    return handle;
}

static NCounters::TNetClassifierCounters::TPtr ExtractCounters(TServer& server) {
    UNIT_ASSERT_VALUES_EQUAL(server.GetRuntime()->GetNodeCount(), 1);

    return MakeIntrusive<NCounters::TNetClassifierCounters>(
        GetServiceCounters(server.GetRuntime()->GetAppData(0).Counters, "netclassifier")->GetSubgroup("subsystem", "distributor")
    );
}

Y_UNIT_TEST_SUITE(TNetClassifierTest) {
    Y_UNIT_TEST(TestInitFromFile) {
        TPortManager pm;
        const ui16 port = pm.GetPort(2134);

        TServerSettings settings(port);

        auto netDataFile = CreateNetDataFile(FormNetData());
        settings.NetClassifierConfig.SetNetDataFilePath(netDataFile->Name());
        settings.NetClassifierConfig.SetTimedCountersUpdateIntervalSeconds(1);

        TServer cleverServer = TServer(settings);
        {
            const TActorId sender = cleverServer.GetRuntime()->AllocateEdgeActor();
            auto handle = GetClassifierUpdate(cleverServer, sender);
            auto* event = handle->Get<TEvNetClassifier::TEvClassifierUpdate>();

            UNIT_ASSERT(event->NetDataUpdateTimestamp);
            UNIT_ASSERT(*event->NetDataUpdateTimestamp > TInstant::Zero());

            // basic sanity check
            UNIT_ASSERT_STRINGS_EQUAL(*event->Classifier->ClassifyAddress("37.29.21.135"), "MYT");
            UNIT_ASSERT(!event->Classifier->ClassifyAddress("trololo"));
        }
        {
            auto counters = ExtractCounters(cleverServer);

            UNIT_ASSERT_VALUES_EQUAL(counters->SubscribersCount->GetAtomic(), 1);
            UNIT_ASSERT_VALUES_EQUAL(counters->GoodConfigNotificationsCount->GetAtomic(), 0);
            UNIT_ASSERT_VALUES_EQUAL(counters->BrokenConfigNotificationsCount->GetAtomic(), 2);
            UNIT_ASSERT_VALUES_EQUAL(counters->NetDataSourceType->GetAtomic(), ENetDataSourceType::File);

            const auto prevLagSeconds = AtomicGet(counters->NetDataUpdateLagSeconds->GetAtomic());
            while (prevLagSeconds >= AtomicGet(counters->NetDataUpdateLagSeconds->GetAtomic())) {
                Wait();
            }
        }
    }

    Y_UNIT_TEST(TestInitFromBadlyFormattedFile) {
        TPortManager pm;
        const ui16 port = pm.GetPort(2134);

        TServerSettings settings(port);

        auto netDataFile = CreateNetDataFile("omg\tlol");
        settings.NetClassifierConfig.SetNetDataFilePath(netDataFile->Name());

        TServer cleverServer = TServer(settings);
        {
            const TActorId sender = cleverServer.GetRuntime()->AllocateEdgeActor();
            auto handle = GetClassifierUpdate(cleverServer, sender);
            auto* event = handle->Get<TEvNetClassifier::TEvClassifierUpdate>();

            UNIT_ASSERT(!event->NetDataUpdateTimestamp);
            UNIT_ASSERT(!event->Classifier);
        }
        {
            auto counters = ExtractCounters(cleverServer);

            UNIT_ASSERT_VALUES_EQUAL(counters->SubscribersCount->GetAtomic(), 1);
            UNIT_ASSERT_VALUES_EQUAL(counters->GoodConfigNotificationsCount->GetAtomic(), 0);
            UNIT_ASSERT_VALUES_EQUAL(counters->BrokenConfigNotificationsCount->GetAtomic(), 2);
            UNIT_ASSERT_VALUES_EQUAL(counters->NetDataSourceType->GetAtomic(), ENetDataSourceType::None);
        }
    }

    static NHttp::TEvHttpProxy::TEvHttpOutgoingResponse* MakeHttpResponse(NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request, const TString& netData) {
        const TString content = TStringBuilder() << "HTTP/1.1 200 OK\r\nConnection: Close\r\nContent-Type: application/octet-stream\r\nContent-Length: "
                                                 << netData.size() << "\r\n\r\n" << netData;
        NHttp::THttpOutgoingResponsePtr httpResponse = request->Request->CreateResponseString(content);

        return new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse);
    }

    Y_UNIT_TEST(TestInitFromRemoteSource) {
        auto sensors = std::make_shared<NMonitoring::TMetricRegistry>();

        TPortManager pm;
        const ui16 port = pm.GetPort(2134);
        const ui64 netDataSourcePort = pm.GetPort(13334);

        const TString hostAndPort = TStringBuilder() << "http://[::1]:" << netDataSourcePort;
        const TString uri = "/fancy_path/networks.tsv";

        TServerSettings settings(port);
        auto& updaterConfig = *settings.NetClassifierConfig.MutableUpdaterConfig();
        updaterConfig.SetNetDataSourceUrl(hostAndPort + uri);
        updaterConfig.SetNetDataUpdateIntervalSeconds(1);
        updaterConfig.SetRetryIntervalSeconds(1);

        settings.NetClassifierConfig.SetTimedCountersUpdateIntervalSeconds(1);

        TServer cleverServer = TServer(settings);
        auto& actorSystem = *cleverServer.GetRuntime();

        NActors::IActor* proxy = NHttp::CreateHttpProxy(sensors);
        NActors::TActorId proxyId = actorSystem.Register(proxy);

        actorSystem.Send(
            new NActors::IEventHandle(proxyId, TActorId(), new NHttp::TEvHttpProxy::TEvAddListeningPort(netDataSourcePort)), 0, true
        );

        NActors::TActorId serverId = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(proxyId, serverId, new NHttp::TEvHttpProxy::TEvRegisterHandler(uri, serverId)), 0, true);

        const TActorId sender = cleverServer.GetRuntime()->AllocateEdgeActor();

        // initially classifier's data should be empty
        auto handle = GetClassifierUpdate(cleverServer, sender);
        UNIT_ASSERT(!handle->Get<TEvNetClassifier::TEvClassifierUpdate>()->Classifier);

        // then the server responds with new net data
        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);
        UNIT_ASSERT_EQUAL(request->Request->URL, uri);

        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, MakeHttpResponse(request, FormNetData())), 0, true);

        // subscriber waits for proper net data update
        while (true) {
            handle = GetClassifierUpdate(cleverServer, sender);

            if (handle->Get<TEvNetClassifier::TEvClassifierUpdate>()->Classifier) {
                break;
            }

            Wait();
        }

        UNIT_ASSERT(handle);
        auto* event = handle->Get<TEvNetClassifier::TEvClassifierUpdate>();

        UNIT_ASSERT_STRINGS_EQUAL(*event->Classifier->ClassifyAddress("185.32.186.26"), "IVA");

        {
            auto counters = ExtractCounters(cleverServer);

            UNIT_ASSERT_VALUES_EQUAL(AtomicGet(counters->SubscribersCount->GetAtomic()), 1);
            UNIT_ASSERT_VALUES_EQUAL(AtomicGet(counters->NetDataSourceType->GetAtomic()), ENetDataSourceType::DistributableConfig);

            const auto prevLagSeconds = AtomicGet(counters->NetDataUpdateLagSeconds->GetAtomic());
            while (prevLagSeconds >= AtomicGet(counters->NetDataUpdateLagSeconds->GetAtomic())) {
                Wait();
            }
        }
    }
}

} // namespace NKikimr::NNetClassifierTests
