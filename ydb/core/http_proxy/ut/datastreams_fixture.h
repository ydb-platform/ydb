#pragma once

#include <ydb/library/actors/http/http_proxy.h>
#include <library/cpp/json/json_reader.h>
#include <ydb/library/grpc/server/actors/logger.h>
#include <library/cpp/http/misc/parsed_request.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/env.h>

#include <ydb/core/http_proxy/discovery_actor.h>
#include <ydb/core/http_proxy/events.h>
#include <ydb/core/http_proxy/grpc_service.h>
#include <ydb/core/http_proxy/http_req.h>
#include <ydb/core/http_proxy/http_service.h>
#include <ydb/core/http_proxy/metrics_actor.h>
#include <ydb/core/mon/sync_http_mon.h>

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/persqueue/tests/counters.h>
#include <ydb/library/testlib/service_mocks/access_service_mock.h>

#include <ydb/public/sdk/cpp/client/ydb_common_client/settings.h>
#include <ydb/public/sdk/cpp/client/ydb_datastreams/datastreams.h>
#include <ydb/public/sdk/cpp/client/ydb_discovery/discovery.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>

#include <ydb/services/ydb/ydb_common_ut.h>

#include <nlohmann/json.hpp>

#include <ydb/core/http_proxy/auth_factory.h>


using TJMap = NJson::TJsonValue::TMapType;
using TJVector = NJson::TJsonValue::TArray;


struct THttpResult {
    ui32 HttpCode;
    TString Description;
    TString Body;
};


template <typename T>
T GetByPath(const NJson::TJsonValue& msg, TStringBuf path) {
    NJson::TJsonValue ret;
    UNIT_ASSERT_C(msg.GetValueByPath(path, ret), path);
    if constexpr (std::is_same<T, TString>::value) {
        return ret.GetStringSafe();
    }
    if constexpr (std::is_same<T, TJMap>::value) {
        return ret.GetMapSafe();
    }
    if constexpr (std::is_same<T, i64>::value) {
        return ret.GetIntegerSafe();
    }
    if constexpr (std::is_same<T, TJVector>::value) {
        return ret.GetArraySafe();
    }
}


class THttpProxyTestMock : public NUnitTest::TBaseFixture {
public:
    THttpProxyTestMock() = default;
    ~THttpProxyTestMock() = default;

    void TearDown(NUnitTest::TTestContext&) override {
        GRpcServer->Stop();
    }

    void SetUp(NUnitTest::TTestContext&) override {
        InitAll();
    }

    void InitAll() {
        AccessServicePort = PortManager.GetPort(8443);
        AccessServiceEndpoint = "127.0.0.1:" + ToString(AccessServicePort);
        InitKikimr();
        InitAccessServiceService();
        InitHttpServer();
    }

    static TString FormAuthorizationStr(const TString& region) {
        return TStringBuilder() <<
            "Authorization: AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/" << region <<
            "/service/aws4_request, SignedHeaders=host;x-amz-date, Signature="
            "5da7c1a2acd57cee7505fc6676e4e544621c30862966e37dddb68e92efbe5d6b)__";
    }

    static NJson::TJsonValue CreateCreateStreamRequest() {
        NJson::TJsonValue record;
        record["StreamName"] = "testtopic";
        record["ShardCount"] = 5;
        return record;
    }

    static NJson::TJsonValue CreateDeleteStreamRequest() {
        NJson::TJsonValue record;
        record["StreamName"] = "testtopic";
        return record;
    }

    static NJson::TJsonValue CreateDescribeStreamRequest() {
        NJson::TJsonValue record;
        record["StreamName"] = "testtopic";
        return record;
    }

    static NJson::TJsonValue CreateDescribeStreamSummaryRequest() {
        NJson::TJsonValue record;
        record["StreamName"] = "testtopic";
        return record;
    }

    static NJson::TJsonValue CreatePutRecordsRequest() {
        NJson::TJsonValue record;
        record["StreamName"] = "testtopic";
        record["Records"] = NJson::TJsonValue(NJson::JSON_ARRAY);
        record["Records"].AppendValue(NJson::TJsonValue(NJson::JSON_MAP));
        record["Records"].Back()["PartitionKey"] = "key";
        record["Records"].Back()["Data"] = Base64Encode("data");
        return record;
    }


    static NJson::TJsonValue CreateRegisterStreamConsumerRequest() {
        NJson::TJsonValue record;
        record["StreamArn"] = "";
        record["ConsumerName"] = "";
        return record;
    }

    static NJson::TJsonValue CreateDeregisterStreamConsumerRequest() {
        NJson::TJsonValue record;
        record["StreamArn"] = "";
        record["ConsumerName"] = "";
        return record;
    }

    static NJson::TJsonValue CreateListStreamConsumersRequest() {
        NJson::TJsonValue record;
        record["StreamArn"] = "testtopic";
        record["MaxResults"] = 100;
        record["NextToken"] = "";
        return record;
    }

    static NJson::TJsonValue CreateGetShardIteratorRequest() {
        NJson::TJsonValue record;
        record["StreamName"] = "testtopic";
        record["ShardId"] = "shard-000000";
        record["StartingSequenceNumber"] = "0000";
        record["Timestamp"] = 0;
        record["ShardIteratorType"] = "LATEST";
        return record;
    }

    static NJson::TJsonValue CreateGetRecordsRequest() {
        NJson::TJsonValue record;
        record["ShardIterator"] = "fill_it_with_response_from_GetShardIterator";
        record["Limit"] = 10000;
        return record;
    }

    static NJson::TJsonValue CreateListShardsRequest() {
        NJson::TJsonValue record;
        record["MaxResults"] = 100;
        record["ShardFilter"]["Type"] = "SHARD_TYPE_UNDEFINED";
        record["ShardFilter"]["ShardId"] = "000000";
        record["ShardFilter"]["Timestamp"] = 0;
        record["StreamName"] = "testtopic";
        return record;
    }


    THttpResult SendHttpRequestRaw(const TString& handler, const TString& target,
                                   const IOutputStream::TPart& body, const TString& authorizationStr,
                                   const TString& contentType = "application/json") {
        TNetworkAddress addr("::", HttpServicePort);
        TSocket sock(addr);
        sock.SetSockOpt(IPPROTO_TCP, TCP_USER_TIMEOUT, 1000);
        TSocketOutput so(sock);
        THttpOutput output(&so);

        output.EnableKeepAlive(false);
        output.EnableCompression(false);

        std::vector<IOutputStream::TPart> parts = {
                IOutputStream::TPart(TStringBuf("POST ")),
                IOutputStream::TPart(TStringBuf(handler)),
                IOutputStream::TPart(TStringBuf(" HTTP/1.1")),
                IOutputStream::TPart::CrLf(),
                IOutputStream::TPart(TStringBuf("Host:")),
                IOutputStream::TPart(TStringBuf("example.amazonaws.com")),
                IOutputStream::TPart::CrLf(),
                IOutputStream::TPart(TStringBuf("X-Amz-Target:")),
                IOutputStream::TPart(TStringBuf(target)),
                IOutputStream::TPart::CrLf(),
                IOutputStream::TPart(TStringBuf("X-Amz-Date:")),
                IOutputStream::TPart(TStringBuf("20150830T123600Z")),
                IOutputStream::TPart::CrLf()
        };
        if (!authorizationStr.empty()) {
            parts.push_back(IOutputStream::TPart(TStringBuf(authorizationStr)));
            parts.push_back(IOutputStream::TPart::CrLf());
        }
        if (!contentType.empty()) {
            parts.push_back(IOutputStream::TPart(TStringBuf("Content-Type:")));
            parts.push_back(IOutputStream::TPart(TStringBuf(contentType)));
            parts.push_back(IOutputStream::TPart::CrLf());
        }
        parts.push_back(IOutputStream::TPart::CrLf());
        parts.push_back(body);

        output.Write(&parts[0], parts.size());
        output.Finish();

        TSocketInput si(sock);
        THttpInput input(&si);

        bool gotRequestId{false};
        for (auto& header : input.Headers()) {
            gotRequestId |= header.Name() == "x-amzn-requestid";
        }
        Y_ABORT_UNLESS(gotRequestId);
        ui32 httpCode = ParseHttpRetCode(input.FirstLine());
        TString description(StripString(TStringBuf(input.FirstLine()).After(' ').After(' ')));
        TString responseBody = input.ReadAll();
        Cerr << "Http output full " << responseBody << Endl;
        return {httpCode, description, responseBody};
    }

    THttpResult SendHttpRequestRawSpecified(const TString& handler, const TString& target,
                                   const TString& host, const TString& date, const TString& userAgent,
                                   const TString& acceptEncoding,
                                   const IOutputStream::TPart& body, const TString& authorizationStr,
                                   const TString& contentType = "application/json") {
        TNetworkAddress addr("::", HttpServicePort);
        TSocket sock(addr);
        sock.SetSockOpt(IPPROTO_TCP, TCP_USER_TIMEOUT, 1000);
        TSocketOutput so(sock);
        THttpOutput output(&so);

        output.EnableKeepAlive(false);
        output.EnableCompression(false);

        std::vector<IOutputStream::TPart> parts = {
                IOutputStream::TPart(TStringBuf("POST ")),
                IOutputStream::TPart(TStringBuf(handler)),
                IOutputStream::TPart(TStringBuf(" HTTP/1.1")),
                IOutputStream::TPart::CrLf(),
                IOutputStream::TPart(TStringBuf("Host:")),
                IOutputStream::TPart(TStringBuf(host)),
                IOutputStream::TPart::CrLf(),
                IOutputStream::TPart(TStringBuf("User-Agent:")),
                IOutputStream::TPart(TStringBuf(userAgent)),
                IOutputStream::TPart::CrLf(),
                IOutputStream::TPart(TStringBuf("X-Amz-Target:")),
                IOutputStream::TPart(TStringBuf(target)),
                IOutputStream::TPart::CrLf(),
                IOutputStream::TPart(TStringBuf("X-Amz-Date:")),
                IOutputStream::TPart(TStringBuf(date)),
                IOutputStream::TPart::CrLf(),
                IOutputStream::TPart(TStringBuf("Accept-Encoding:")),
                IOutputStream::TPart(TStringBuf(acceptEncoding)),
                IOutputStream::TPart::CrLf()
        };
        if (!authorizationStr.empty()) {
            parts.push_back(IOutputStream::TPart(TStringBuf("Authorization:")));
            parts.push_back(IOutputStream::TPart(TStringBuf(authorizationStr)));
            parts.push_back(IOutputStream::TPart::CrLf());
        }
        if (!contentType.empty()) {
            parts.push_back(IOutputStream::TPart(TStringBuf("Content-Type:")));
            parts.push_back(IOutputStream::TPart(TStringBuf(contentType)));
            parts.push_back(IOutputStream::TPart::CrLf());
        }
        parts.push_back(IOutputStream::TPart::CrLf());
        parts.push_back(body);

        output.Write(&parts[0], parts.size());
        output.Finish();

        TSocketInput si(sock);
        THttpInput input(&si);

        ui32 httpCode = ParseHttpRetCode(input.FirstLine());
        TString description(StripString(TStringBuf(input.FirstLine()).After(' ').After(' ')));
        TString responseBody = input.ReadAll();
        Cerr << "Http output full " << responseBody << Endl;
        return {httpCode, description, responseBody};
    }

    THttpResult SendHttpRequest(const TString& handler, const TString& target, NJson::TJsonValue value,
                                const TString& authorizationStr,
                                const TString& contentType = "application/json") {
        TString jsonStr = NJson::WriteJson(value);
        return SendHttpRequestRaw(handler, target, {&jsonStr[0], jsonStr.size()}, authorizationStr, contentType);
    }

    THttpResult SendHttpRequestSpecified(const TString& handler, const TString& target, NJson::TJsonValue value,
                                const TString& host, const TString& date, const TString& userAgent,
                                const TString& acceptEncoding, const TString& authorizationStr,
                                const TString& contentType = "application/json") {
        TString jsonStr = NJson::WriteJson(value);
        return SendHttpRequestRawSpecified(handler, target, host, date, userAgent, acceptEncoding,
                                  {&jsonStr[0], jsonStr.size()}, authorizationStr, contentType);
    }

    THttpResult SendPing() {
        TNetworkAddress addr("::", HttpServicePort);
        TSocket sock(addr);
        SendMinimalHttpRequest(sock, TStringBuilder() << "[::]:" << HttpServicePort, "/Ping");
        TSocketInput si(sock);
        THttpInput input(&si);
        ui32 httpCode = ParseHttpRetCode(input.FirstLine());
        return {httpCode, "", ""};
    }

private:
    void InitKikimr() {
        AuthFactory = std::make_shared<TIamAuthFactory>();
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutablePQConfig()->SetTopicsAreFirstClassCitizen(true);
        appConfig.MutablePQConfig()->SetEnabled(true);
        appConfig.MutablePQConfig()->AddValidWriteSpeedLimitsKbPerSec(128);
        appConfig.MutablePQConfig()->AddValidWriteSpeedLimitsKbPerSec(512);
        appConfig.MutablePQConfig()->AddValidWriteSpeedLimitsKbPerSec(1_KB);
        appConfig.MutablePQConfig()->MutableBillingMeteringConfig()->SetEnabled(true);

        auto limit = appConfig.MutablePQConfig()->AddValidRetentionLimits();
        limit->SetMinPeriodSeconds(0);
        limit->SetMaxPeriodSeconds(TDuration::Days(1).Seconds());
        limit->SetMinStorageMegabytes(0);
        limit->SetMaxStorageMegabytes(0);

        limit = appConfig.MutablePQConfig()->AddValidRetentionLimits();
        limit->SetMinPeriodSeconds(0);
        limit->SetMaxPeriodSeconds(TDuration::Days(7).Seconds());
        limit->SetMinStorageMegabytes(50_KB);
        limit->SetMaxStorageMegabytes(1_MB);

        NYdb::TKikimrWithGrpcAndRootSchema* server =
            new NYdb::TKikimrWithGrpcAndRootSchema(std::move(appConfig), {}, {}, false, nullptr,
                [this](NYdb::TServerSettings& settings) -> void {
                    settings.SetDataStreamsAuthFactory(AuthFactory);
                    settings.CreateTicketParser = CreateTicketParser;
                    Y_ABORT_UNLESS(AccessServiceEndpoint);
                    settings.AuthConfig.SetAccessServiceEndpoint(AccessServiceEndpoint);
                    settings.AuthConfig.SetUseAccessService(true);
                    settings.AuthConfig.SetUseAccessServiceTLS(false);
                }, 0, 1);

        server->ServerSettings->SetUseRealThreads(false);
        KikimrServer = THolder<NYdb::TKikimrWithGrpcAndRootSchema>(server);

        ActorRuntime = KikimrServer->GetRuntime();

        ActorRuntime->SetLogPriority(NKikimrServices::GRPC_PROXY, NLog::PRI_DEBUG);
        ActorRuntime->SetLogPriority(NKikimrServices::PERSQUEUE, NLog::PRI_DEBUG);
        ActorRuntime->SetLogPriority(NKikimrServices::HTTP_PROXY, NLog::PRI_DEBUG);
        ActorRuntime->SetLogPriority(NActorsServices::EServiceCommon::HTTP, NLog::PRI_DEBUG);
        ActorRuntime->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);

        NYdb::TClient client(*(KikimrServer->ServerSettings));
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 client.AlterUserAttributes("/", "Root", {{"folder_id", "folder4"},
                                                                          {"cloud_id", "cloud4"},
                                                                          {"database_id", "database4"}}));
        NACLib::TDiffACL acl;
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericFull, "Service1_id@as");
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericFull, "proxy_sa@as");

        client.ModifyACL("/", "Root", acl.SerializeAsString());
    }

    void InitAccessServiceService() {
        // Service Account Service Mock
        grpc::ServerBuilder builder;
        AccessServiceMock.AuthenticateData["kinesis"].Response.mutable_subject()->mutable_service_account()->set_id("Service1_id");
//        AccessServiceMock.AuthenticateData["proxy_sa@builtin"].Response.mutable_subject()->mutable_service_account()->set_id("Service1_id");

        AccessServiceMock.AuthorizeData["AKIDEXAMPLE-ydb.databases.list-folder4"].Response.mutable_subject()->mutable_service_account()->set_id("Service1_id");
        AccessServiceMock.AuthorizeData["proxy_sa@builtin-ydb.databases.list-folder4"].Response.mutable_subject()->mutable_service_account()->set_id("Service1_id");

        AccessServiceMock.AuthorizeData["AKIDEXAMPLE-ydb.databases.list-database4"].Response.mutable_subject()->mutable_service_account()->set_id("Service1_id");
        AccessServiceMock.AuthorizeData["proxy_sa@builtin-ydb.databases.list-database4"].Response.mutable_subject()->mutable_service_account()->set_id("Service1_id");

        builder.AddListeningPort(AccessServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&AccessServiceMock);
        AccessServiceServer = builder.BuildAndStart();
    }

    void InitHttpServer() {
        NKikimrConfig::TServerlessProxyConfig config;
        config.MutableHttpConfig()->AddYandexCloudServiceRegion("ru-central1");
        config.MutableHttpConfig()->AddYandexCloudServiceRegion("ru-central-1");
        HttpServicePort = PortManager.GetPort(2139);
        config.MutableHttpConfig()->SetIamTokenServiceEndpoint(TStringBuilder() << "127.0.0.1:" << IamTokenServicePort);
        config.SetDatabaseServiceEndpoint(TStringBuilder() << "127.0.0.1:" << DatabaseServicePort);
        config.MutableHttpConfig()->SetAccessServiceEndpoint(TStringBuilder() << "127.0.0.1:" << AccessServicePort);
        config.SetTestMode(true);
        config.MutableHttpConfig()->SetPort(HttpServicePort);

        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory = NYdb::CreateOAuthCredentialsProviderFactory("proxy_sa@builtin");

        std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider = credentialsProviderFactory->CreateProvider();

        MonPort = TPortManager().GetPort();
        Counters = new NMonitoring::TDynamicCounters();

        Monitoring.Reset(new NActors::TSyncHttpMon({
            .Port = MonPort,
            .Address = "127.0.0.1",
            .Threads = 3,
            .Title = "whatever",
            .Host = "127.0.0.1",
        }));
        Monitoring->RegisterCountersPage("counters", "Counters", Counters);
        Monitoring->Start();

        Sleep(TDuration::Seconds(1));

        GRpcServerPort = PortManager.GetPort(2140);

        NYdbGrpc::TServerOptions opts;
        opts.SetHost("127.0.0.1");
        opts.SetPort(GRpcServerPort);
        opts.SetWorkerThreads(1);

        auto as = ActorRuntime->GetAnyNodeActorSystem();
        opts.SetLogger(NYdbGrpc::CreateActorSystemLogger(*as, NKikimrServices::GRPC_SERVER));

        TActorId actorId = as->Register(CreateAccessServiceActor(config));
        as->RegisterLocalService(MakeAccessServiceID(), actorId);

        actorId = as->Register(CreateIamTokenServiceActor(config));
        as->RegisterLocalService(MakeIamTokenServiceID(), actorId);

        actorId = as->Register(CreateDiscoveryProxyActor(credentialsProvider, config));
        as->RegisterLocalService(MakeDiscoveryProxyID(), actorId);

        actorId = as->Register(CreateMetricsActor(TMetricsSettings{Counters}));
        as->RegisterLocalService(MakeMetricsServiceID(), actorId);

        actorId = as->Register(NHttp::CreateHttpProxy());
        as->RegisterLocalService(MakeHttpServerServiceID(), actorId);

        THttpProxyConfig httpProxyConfig;
        httpProxyConfig.Config = config;
        httpProxyConfig.CredentialsProvider = credentialsProvider;
        httpProxyConfig.UseSDK = GetEnv("INSIDE_YDB").empty();
        actorId = as->Register(NKikimr::NHttpProxy::CreateHttpProxy(httpProxyConfig));
        as->RegisterLocalService(MakeHttpProxyID(), actorId);

        GRpcServer = MakeHolder<NYdbGrpc::TGRpcServer>(opts);
        GRpcServer->AddService(new NKikimr::NHttpProxy::TGRpcDiscoveryService(as, credentialsProvider, Counters));
        GRpcServer->Start();

        Sleep(TDuration::Seconds(1));
    }


public:
    std::shared_ptr<NKikimr::NHttpProxy::IAuthFactory> AuthFactory;
    THolder<NYdb::TKikimrWithGrpcAndRootSchema> KikimrServer;
    TPortManager PortManager;
    TTestActorRuntime* ActorRuntime = nullptr;
    TAccessServiceMock AccessServiceMock;
    TString AccessServiceEndpoint;
    std::unique_ptr<grpc::Server> AccessServiceServer;
    std::unique_ptr<grpc::Server> IamTokenServer;
    std::unique_ptr<grpc::Server> DatabaseServiceServer;
    TAutoPtr<TMon> Monitoring;
    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters = {};
    THolder<NYdbGrpc::TGRpcServer> GRpcServer;
    ui16 GRpcServerPort = 0;
    ui16 HttpServicePort = 0;
    ui16 AccessServicePort = 0;
    ui16 IamTokenServicePort = 0;
    ui16 DatabaseServicePort = 0;
    ui16 MonPort = 0;
};
