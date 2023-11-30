#include <ydb/core/cms/console/defs.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/cms/console/net_classifier_updater.h>
#include <ydb/core/testlib/test_client.h>

#include <ydb/library/actors/http/http_proxy.cpp>

#include <library/cpp/protobuf/util/is_equal.h>

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/json/json_writer.h>

#include <util/string/builder.h>

namespace NKikimr::NNetClassifierUpdaterTests {

using namespace NConsole;
using namespace Tests;

using TNetClassifierUpdaterConfig = NKikimrNetClassifier::TNetClassifierUpdaterConfig;
const TString NETWORKS_URI = "/fancy_path/networks.tsv";

static NHttp::TEvHttpProxy::TEvHttpOutgoingResponse* MakeHttpResponse(NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request, const TString& netData) {
    const TString content = TStringBuilder() << "HTTP/1.1 200 OK\r\nConnection: Close\r\nContent-Type: application/octet-stream\r\nContent-Length: "
                                             << netData.size() << "\r\n\r\n" << netData;
    NHttp::THttpOutgoingResponsePtr httpResponse = request->Request->CreateResponseString(content);

    return new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse);
}

template<typename TDistributableConfProto>
bool CheckDistributableConfig(const TDistributableConfProto& config, const NKikimrNetClassifier::TNetData& expectedNetData) {
    if (config.GetPackedNetData()) {
        UNIT_ASSERT(config.GetLastUpdateDatetimeUTC());
        UNIT_ASSERT(config.GetLastUpdateTimestamp());

        UNIT_ASSERT_STRINGS_EQUAL(
            TInstant::MicroSeconds(config.GetLastUpdateTimestamp()).ToRfc822String(),
            config.GetLastUpdateDatetimeUTC()
        );

        const TString serializedNetData = NNetClassifierUpdater::UnpackNetData(config.GetPackedNetData());
        UNIT_ASSERT(serializedNetData);

        NKikimrNetClassifier::TNetData netData;
        UNIT_ASSERT(netData.ParseFromString(serializedNetData));

        UNIT_ASSERT(NProtoBuf::IsEqual(netData, expectedNetData));

        return true;
    }

    return false;
}

static NKikimrNetClassifier::TNetData FormNetData() {
    NKikimrNetClassifier::TNetData netData;

    {
        auto& subnet = *netData.AddSubnets();
        subnet.SetMask("2a02:6b8:fc0a::/48");
        subnet.SetLabel("SAS");
    }
    {
        auto& subnet = *netData.AddSubnets();
        subnet.SetMask("87.250.239.224/31");
        subnet.SetLabel("VLA");
    }

    return netData;
}

static TString ConvertToTsv(const NKikimrNetClassifier::TNetData& netData) {
    TStringBuilder builder;

    for (size_t i = 0; i < netData.SubnetsSize(); ++i) {
        const auto& subnet = netData.GetSubnets(i);
        if (i) {
            builder << "\n";
        }
        builder << subnet.GetMask() << "\t" << subnet.GetLabel();
    }

    return builder;
}


static TString ConvertToJson(const NKikimrNetClassifier::TNetData& netData) {

    TString res;
    TStringOutput ss(res);

    NJson::TJsonWriter writer(&ss, true);

    writer.OpenMap();
    writer.Write("count", netData.SubnetsSize());
    writer.OpenArray("results");
    for (size_t i = 0; i < netData.SubnetsSize(); ++i) {
        const auto& subnet = netData.GetSubnets(i);
        writer.OpenMap();
        writer.Write("prefix",  subnet.GetMask());
        writer.OpenArray("tags");
        writer.Write(subnet.GetLabel());
        writer.CloseArray();
        writer.CloseMap();
    }
    writer.CloseArray();
    writer.CloseMap();
    writer.Flush();
    ss.Flush();
    return res;
}

NKikimrNetClassifier::TNetClassifierUpdaterConfig CreateUpdaterConfig(
    ui16 netDataSourcePort,
    TNetClassifierUpdaterConfig::EFormat format,
    const TVector<TString>& netBoxTags = {}
) {
    const TString url = TStringBuilder() << "http://[::1]:" << netDataSourcePort <<  NETWORKS_URI;
    NKikimrNetClassifier::TNetClassifierUpdaterConfig updaterConfig;
    updaterConfig.SetNetDataSourceUrl(url);
    updaterConfig.SetFormat(format);
    *updaterConfig.MutableNetBoxTags() = {netBoxTags.begin(), netBoxTags.end()};
    return updaterConfig;
}

Y_UNIT_TEST_SUITE(TNetClassifierUpdaterTest) {
    void TestGetUpdatesFromHttpServer(
        const TString& sourceResponce,
        const NKikimrNetClassifier::TNetData& expectedNetData,
        TNetClassifierUpdaterConfig::EFormat format = TNetClassifierUpdaterConfig::TSV,
        const TVector<TString>& netBoxTags = {}
    ) {
        auto sensors(std::make_shared<NMonitoring::TMetricRegistry>());

        TPortManager pm;
        const ui16 port = pm.GetPort(2134);
        const ui64 netDataSourcePort = pm.GetPort(13334);
        TServerSettings settings(port);
        auto& updaterConfig = *settings.NetClassifierConfig.MutableUpdaterConfig();
        updaterConfig =  CreateUpdaterConfig(netDataSourcePort, format, netBoxTags);
        TServer cleverServer = TServer(settings);
        auto& actorSystem = *cleverServer.GetRuntime();

        NActors::IActor* proxy = NHttp::CreateHttpProxy(sensors);
        NActors::TActorId proxyId = actorSystem.Register(proxy);

        actorSystem.Send(
            new NActors::IEventHandle(proxyId, TActorId(), new NHttp::TEvHttpProxy::TEvAddListeningPort(netDataSourcePort)), 0, true
        );

        NActors::TActorId serverId = actorSystem.AllocateEdgeActor();

        actorSystem.Send(new NActors::IEventHandle(proxyId, serverId, new NHttp::TEvHttpProxy::TEvRegisterHandler(NETWORKS_URI, serverId)), 0, true);

        TAutoPtr<NActors::IEventHandle> handle;
        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);
        UNIT_ASSERT_EQUAL(request->Request->URL, NETWORKS_URI);

        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, MakeHttpResponse(request, sourceResponce)), 0, true);
        const TActorId sender = actorSystem.AllocateEdgeActor();

        size_t iterations = 0;
        while (true) {
            UNIT_ASSERT(++iterations < 60);

            const auto kind = static_cast<ui32>(NKikimrConsole::TConfigItem::NetClassifierDistributableConfigItem);
            actorSystem.Send(
                new IEventHandle(MakeConfigsDispatcherID(sender.NodeId()), sender,
                    new TEvConfigsDispatcher::TEvGetConfigRequest(kind)
                ));

            const auto event = cleverServer.GetRuntime()->GrabEdgeEvent<TEvConfigsDispatcher::TEvGetConfigResponse>(handle);

            if (CheckDistributableConfig(event->Config->GetNetClassifierDistributableConfig(), expectedNetData)) {
                break;
            }

            // wait for the proper update
            Sleep(TDuration::Seconds(1));
        }
    }

    Y_UNIT_TEST(TestGetUpdatesFromHttpServer) {
        auto netData = FormNetData();
        TestGetUpdatesFromHttpServer(ConvertToTsv(netData), netData);
        TestGetUpdatesFromHttpServer(ConvertToJson(netData), netData, TNetClassifierUpdaterConfig::NETBOX);
    }

    void RunNetBoxTest(const TString& netboxResponce, const TVector<TString>& tags, const TVector<std::pair<TString, TString>>& expected) {
        auto addMask = [](NKikimrNetClassifier::TNetData& data, const TString& mask, const TString& label) {
            auto& subnet = *data.AddSubnets();
            subnet.SetMask(mask);
            subnet.SetLabel(label);
        };
        NKikimrNetClassifier::TNetData data;
        for (auto& net : expected) {
            addMask(data, net.first, net.second);
        }
        TestGetUpdatesFromHttpServer(netboxResponce, data, TNetClassifierUpdaterConfig::NETBOX, tags);
    }

    void RunNetBoxCommonTests(const TString& netboxResponce) {
        RunNetBoxTest(
            netboxResponce,
            {},
            {
                {"5.45.192.0/18", "asd"},
                {"5.255.192.0/18", "zxcv"},
                {"37.9.64.0/18", "zxcv"},
                {"95.108.128.0/17", "asd"},
                {"172.24.0.0/13", "qwerty"}
            }
        );
        RunNetBoxTest(
            netboxResponce,
            {"asd2", "asd", "zxcv", "qwerty", "faketag"},
            {
                {"5.45.192.0/18", "asd"},
                {"5.255.192.0/18", "zxcv"},
                {"37.9.64.0/18", "zxcv"},
                {"95.108.128.0/17", "asd"},
                {"172.24.0.0/13", "qwerty"}
            }
        );
        RunNetBoxTest(
            netboxResponce,
            {"asd", "zxcv"},
            {
                {"5.45.192.0/18", "asd"},
                {"5.255.192.0/18", "zxcv"},
                {"37.9.64.0/18", "zxcv"},
                {"95.108.128.0/17", "asd"}
            }
        );
        RunNetBoxTest(
            netboxResponce,
            {"zxcv", "asd"},
            {
                {"5.45.192.0/18", "asd"},
                {"5.255.192.0/18", "zxcv"},
                {"37.9.64.0/18", "zxcv"},
                {"95.108.128.0/17", "asd"}
            }
        );
        RunNetBoxTest(
            netboxResponce,
            {"qwerty"},
            {
                {"172.24.0.0/13", "qwerty"}
            }
        );
    }

    void RunTestWithCastomFields(const TString& netboxResponce) {
        RunNetBoxCommonTests(netboxResponce);
        RunNetBoxTest(
            netboxResponce,
            {"asd"},
            {
                {"5.45.192.0/18", "asd"},
                {"95.108.128.0/17", "asd"}
            }
        );
        RunNetBoxTest(
            netboxResponce,
            {"zxcv"},
            {
                {"5.255.192.0/18", "zxcv"},
                {"37.9.64.0/18", "zxcv"}
            }
        );
    }

    Y_UNIT_TEST(TestFiltrationByNetboxCustomFieldsAndTags) {
        const TString netboxResponce = R"__(
            {
                "count": 5,
                "results": [
                    {"prefix": "5.45.192.0/18", "custom_fields": {"owner": "asd"}, "tags": ["asd", "zxcv"]},
                    {"prefix": "5.255.192.0/18", "custom_fields": {"owner": "zxcv"}, "tags": ["zxcv", "asd"]},
                    {"prefix": "37.9.64.0/18", "custom_fields": {"owner": "zxcv"}, "tags": ["zxcv"]},
                    {"prefix": "95.108.128.0/17", "custom_fields": {"owner": "asd"}, "tags": ["asd"]},
                    {"prefix": "172.24.0.0/13", "custom_fields": {"owner": "qwerty"}, "tags": ["qwerty"]}
                ]
            }
        )__";
        RunTestWithCastomFields(netboxResponce);
    }

    Y_UNIT_TEST(TestFiltrationByNetboxCustomFieldsOnly) {
        const TString netboxResponce = R"__(
            {
                "count": 5,
                "results": [
                    {"prefix": "5.45.192.0/18", "custom_fields": {"owner": "asd"}},
                    {"prefix": "5.255.192.0/18", "custom_fields": {"owner": "zxcv"}},
                    {"prefix": "37.9.64.0/18", "custom_fields": {"owner": "zxcv"}},
                    {"prefix": "95.108.128.0/17", "custom_fields": {"owner": "asd"}},
                    {"prefix": "172.24.0.0/13", "custom_fields": {"owner": "qwerty"}}
                ]
            }
        )__";

        RunTestWithCastomFields(netboxResponce);
    }

    Y_UNIT_TEST(TestFiltrationByNetboxTags) {
        const TString netboxResponce = R"__(
            {
                "count": 5,
                "results": [
                    {"prefix": "5.45.192.0/18", "tags": ["asd", "zxcv"]},
                    {"prefix": "5.255.192.0/18", "tags": ["zxcv", "asd"]},
                    {"prefix": "37.9.64.0/18", "tags": ["zxcv"]},
                    {"prefix": "95.108.128.0/17", "tags": ["asd"]},
                    {"prefix": "172.24.0.0/13", "tags": ["qwerty"]}
                ]
            }
        )__";
        RunNetBoxCommonTests(netboxResponce);

        RunNetBoxTest(
            netboxResponce,
            {"asd"},
            {
                {"5.45.192.0/18", "asd"},
                {"5.255.192.0/18", "asd"},
                {"95.108.128.0/17", "asd"}
            }
        );
        RunNetBoxTest(
            netboxResponce,
            {"zxcv"},
            {
                {"5.45.192.0/18", "zxcv"},
                {"5.255.192.0/18", "zxcv"},
                {"37.9.64.0/18", "zxcv"}
            }
        );
    }
}

} // namespace NKikimr::NNetClassifierUpdaterTests
