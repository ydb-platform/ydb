#include <ydb/core/fq/libs/actors/database_resolver.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/db_id_async_resolver_impl/mdb_endpoint_generator.h>
#include <ydb/core/fq/libs/config/protos/checkpoint_coordinator.pb.h>

#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/helpers.h>

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/http/http_proxy.h>

namespace {

using namespace NKikimr;
using namespace NFq;

TString MakeErrorPrefix(
    const TString& host, 
    const TString& url,
    const TString& databaseId,
    const NYql::EDatabaseType& databaseType) {
    TStringBuilder ss;
    
    return TStringBuilder() 
        << "Error while trying to resolve managed " << ToString(databaseType)
        << " database with id " << databaseId << " via HTTP request to"
        << ": endpoint '" << host << "'"
        << ", url '" << url << "'"
        << ": ";
}

TString NoPermissionStr = "you have no permission to resolve database id into database endpoint.";

struct TTestBootstrap : public TTestActorRuntime {
    NConfig::TCheckpointCoordinatorConfig Settings;
    NActors::TActorId DatabaseResolver;
    NActors::TActorId HttpProxy;
    NActors::TActorId AsyncResolver;
    THashMap<TActorId, ui64> ActorToTask;

    explicit TTestBootstrap()
        : TTestActorRuntime(true)
    {
        TAutoPtr<TAppPrepare> app = new TAppPrepare();
        
        Initialize(app->Unwrap());
        HttpProxy = AllocateEdgeActor();
        AsyncResolver = AllocateEdgeActor();

        SetLogPriority(NKikimrServices::STREAMS_CHECKPOINT_COORDINATOR, NLog::PRI_DEBUG);
        auto credentialsFactory = NYql::CreateSecuredServiceAccountCredentialsOverTokenAccessorFactory("", true, "");

        DatabaseResolver = Register(CreateDatabaseResolver(
            HttpProxy,
            credentialsFactory
        ));
    }

    void WaitForBootstrap() {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Bootstrap, 1);
        UNIT_ASSERT(DispatchEvents(options));
    }

    void CheckEqual(
        const NHttp::TEvHttpProxy::TEvHttpOutgoingRequest& lhs,
        const NHttp::TEvHttpProxy::TEvHttpOutgoingRequest& rhs) {
        UNIT_ASSERT_EQUAL_C(lhs.Request->URL, rhs.Request->URL, "Compare: " << lhs.Request->URL << " " << rhs.Request->URL);
    }

    void CheckEqual(
        const NYql::TIssue& lhs,
        const NYql::TIssue& rhs) {
        UNIT_ASSERT_VALUES_EQUAL(lhs.GetMessage(), rhs.GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(lhs.GetCode(), rhs.GetCode());
    }

    void CheckEqual(
        const NFq::TEvents::TEvEndpointResponse& lhs,
        const NFq::TEvents::TEvEndpointResponse& rhs) {
        UNIT_ASSERT_VALUES_EQUAL(lhs.DbResolverResponse.Success, rhs.DbResolverResponse.Success);
        UNIT_ASSERT_VALUES_EQUAL(lhs.DbResolverResponse.DatabaseDescriptionMap.size(), rhs.DbResolverResponse.DatabaseDescriptionMap.size());
        for (auto it = lhs.DbResolverResponse.DatabaseDescriptionMap.begin(); it != lhs.DbResolverResponse.DatabaseDescriptionMap.end(); ++it) {
            auto key = it->first;
            UNIT_ASSERT(rhs.DbResolverResponse.DatabaseDescriptionMap.contains(key));
            const NYql::TDatabaseResolverResponse::TDatabaseDescription& lhsDesc = it->second;
            const NYql::TDatabaseResolverResponse::TDatabaseDescription& rhsDesc = rhs.DbResolverResponse.DatabaseDescriptionMap.find(key)->second;
            UNIT_ASSERT_VALUES_EQUAL(lhsDesc.Endpoint, rhsDesc.Endpoint);
            UNIT_ASSERT_VALUES_EQUAL(lhsDesc.Host, rhsDesc.Host);
            UNIT_ASSERT_VALUES_EQUAL(lhsDesc.Port, rhsDesc.Port);
            UNIT_ASSERT_VALUES_EQUAL(lhsDesc.Database, rhsDesc.Database);
            UNIT_ASSERT_VALUES_EQUAL(lhsDesc.Secure, rhsDesc.Secure);
        }

        UNIT_ASSERT_VALUES_EQUAL(lhs.DbResolverResponse.Issues.Size(), rhs.DbResolverResponse.Issues.Size());
        auto lhsIssueIter = lhs.DbResolverResponse.Issues.begin(); 
        auto rhsIssueIter = rhs.DbResolverResponse.Issues.begin(); 
        while (lhsIssueIter != lhs.DbResolverResponse.Issues.end()) {
            CheckEqual(*lhsIssueIter, *rhsIssueIter);
            lhsIssueIter++;
            rhsIssueIter++;
        }
    }

    template <typename TEvent>
    typename TEvent::TPtr ExpectEvent(NActors::TActorId actorId, const TEvent& expectedEventValue, NActors::TActorId* outSenderActorId = nullptr) {
        typename TEvent::TPtr eventHolder = GrabEdgeEvent<TEvent>(actorId, TDuration::Seconds(10));
        UNIT_ASSERT(eventHolder.Get() != nullptr);
        TEvent* actual = eventHolder.Get()->Get();
        CheckEqual(expectedEventValue, *actual);
        if (outSenderActorId) {
            *outSenderActorId = eventHolder->Sender;
        }
        return eventHolder;
    }
   
};
} // namespace

namespace NFq {

Y_UNIT_TEST_SUITE(TDatabaseResolverTests) {

    void Test(
        NYql::EDatabaseType databaseType,
        NYql::NConnector::NApi::EProtocol protocol,
        const TString& getUrl,
        const TString& status,
        const TString& responseBody,
        const NYql::TDatabaseResolverResponse::TDatabaseDescription& description,
        const NYql::TIssues& issues,
        const TString& error = ""
        )
    {
        TTestBootstrap bootstrap;

        NYql::TDatabaseAuth databaseAuth;
        databaseAuth.UseTls = true;
        databaseAuth.Protocol = protocol;

        TString databaseId{"etn021us5r9rhld1vgbh"};
        auto requestIdAndDatabaseType = std::make_pair(databaseId, databaseType);

        bootstrap.Send(new IEventHandle(
            bootstrap.DatabaseResolver,
            bootstrap.AsyncResolver,
            new NFq::TEvents::TEvEndpointRequest(
                NYql::IDatabaseAsyncResolver::TDatabaseAuthMap(
                    {std::make_pair(requestIdAndDatabaseType, databaseAuth)}),
                TString("https://ydbc.ydb.cloud.yandex.net:8789/ydbc/cloud-prod"),
                TString("https://mdb.api.cloud.yandex.net:443"),
                TString("traceId"),
                NFq::MakeMdbEndpointGeneratorGeneric(true))));

        NActors::TActorId processorActorId;
        auto httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet(getUrl);
        auto httpOutgoingRequestHolder = bootstrap.ExpectEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(bootstrap.HttpProxy, NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(
             httpRequest), &processorActorId);

        NHttp::TEvHttpProxy::TEvHttpOutgoingRequest* httpOutgoingRequest = httpOutgoingRequestHolder.Get()->Get();

        bootstrap.WaitForBootstrap();

        std::unique_ptr<NHttp::THttpIncomingResponse> httpIncomingResponse;
        if (!error) {
            httpIncomingResponse = std::make_unique<NHttp::THttpIncomingResponse>(nullptr);
            httpIncomingResponse->Status = status;
            httpIncomingResponse->Body = responseBody;
        }

        bootstrap.Send(new IEventHandle(
            processorActorId,
            bootstrap.HttpProxy,
            new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(httpOutgoingRequest->Request, httpIncomingResponse.release(), error)));

        NYql::TDatabaseResolverResponse::TDatabaseDescriptionMap result;
        if (status == "200") {
            result[requestIdAndDatabaseType] = description;
        }
        bootstrap.ExpectEvent<TEvents::TEvEndpointResponse>(bootstrap.AsyncResolver, 
            NFq::TEvents::TEvEndpointResponse(
                NYql::TDatabaseResolverResponse(std::move(result), status == "200", issues)));
    }
    
    Y_UNIT_TEST(Ydb_Serverless) {
        Test(
            NYql::EDatabaseType::Ydb,
            NYql::NConnector::NApi::EProtocol::PROTOCOL_UNSPECIFIED,
            "https://ydbc.ydb.cloud.yandex.net:8789/ydbc/cloud-prod/database?databaseId=etn021us5r9rhld1vgbh",
            "200",
            R"(
                {
                    "endpoint":"grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1g7jdjqd07qg43c4fmp/etn021us5r9rhld1vgbh"
                })",           
            NYql::TDatabaseResolverResponse::TDatabaseDescription{
                TString{"ydb.serverless.yandexcloud.net:2135"},
                TString{"ydb.serverless.yandexcloud.net"},
                2135,
                TString("/ru-central1/b1g7jdjqd07qg43c4fmp/etn021us5r9rhld1vgbh"),
                true
                },
                {}
            );
    }

    Y_UNIT_TEST(Ydb_Serverless_Timeout) {
        NYql::TIssues issues{
            NYql::TIssue(
                TStringBuilder{} << MakeErrorPrefix(
                    "ydbc.ydb.cloud.yandex.net:8789",
                    "/ydbc/cloud-prod/database?databaseId=etn021us5r9rhld1vgbh",
                    "etn021us5r9rhld1vgbh",
                    NYql::EDatabaseType::Ydb
                ) << "Connection timeout"
            )
        };

        Test(
            NYql::EDatabaseType::Ydb,
            NYql::NConnector::NApi::EProtocol::PROTOCOL_UNSPECIFIED,
            "https://ydbc.ydb.cloud.yandex.net:8789/ydbc/cloud-prod/database?databaseId=etn021us5r9rhld1vgbh",
            "",
            "",           
            NYql::TDatabaseResolverResponse::TDatabaseDescription{
                TString{"ydb.serverless.yandexcloud.net:2135"},
                TString{"ydb.serverless.yandexcloud.net"},
                2135,
                TString("/ru-central1/b1g7jdjqd07qg43c4fmp/etn021us5r9rhld1vgbh"),
                true
                },
                issues,
                "Connection timeout"
            );
    }

    Y_UNIT_TEST(Ydb_Dedicated) {
        Test(
            NYql::EDatabaseType::Ydb,
            NYql::NConnector::NApi::EProtocol::PROTOCOL_UNSPECIFIED,
            "https://ydbc.ydb.cloud.yandex.net:8789/ydbc/cloud-prod/database?databaseId=etn021us5r9rhld1vgbh",
            "200",
            R"(
                {
                    "endpoint":"grpcs://lb.etnbrtlini51k7cinbdr.ydb.mdb.yandexcloud.net:2135/?database=/ru-central1/b1gtl2kg13him37quoo6/etn021us5r9rhld1vgbh", 
                    "storageConfig":{"storageSizeLimit":107374182400}
                })",
            NYql::TDatabaseResolverResponse::TDatabaseDescription{
                TString{"u-lb.etnbrtlini51k7cinbdr.ydb.mdb.yandexcloud.net:2135"},
                TString{"u-lb.etnbrtlini51k7cinbdr.ydb.mdb.yandexcloud.net"},
                2135,
                TString("/ru-central1/b1gtl2kg13him37quoo6/etn021us5r9rhld1vgbh"),
                true
                },
                {}
            );
    }

    Y_UNIT_TEST(DataStreams_Serverless) {
        Test(
            NYql::EDatabaseType::DataStreams,
            NYql::NConnector::NApi::EProtocol::PROTOCOL_UNSPECIFIED,
            "https://ydbc.ydb.cloud.yandex.net:8789/ydbc/cloud-prod/database?databaseId=etn021us5r9rhld1vgbh",
            "200",
            R"(
                {
                    "endpoint":"grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1g7jdjqd07qg43c4fmp/etn021us5r9rhld1vgbh"
                })",
            NYql::TDatabaseResolverResponse::TDatabaseDescription{
                TString{"yds.serverless.yandexcloud.net:2135"},
                TString{"yds.serverless.yandexcloud.net"},
                2135,
                TString("/ru-central1/b1g7jdjqd07qg43c4fmp/etn021us5r9rhld1vgbh"),
                true
            },
            {}
            );
    }

    Y_UNIT_TEST(DataStreams_Dedicated) {
        Test(
            NYql::EDatabaseType::DataStreams,
            NYql::NConnector::NApi::EProtocol::PROTOCOL_UNSPECIFIED,
            "https://ydbc.ydb.cloud.yandex.net:8789/ydbc/cloud-prod/database?databaseId=etn021us5r9rhld1vgbh",
            "200",
            R"(
                {
                    "endpoint":"grpcs://lb.etn021us5r9rhld1vgbh.ydb.mdb.yandexcloud.net:2135/?database=/ru-central1/b1g7jdjqd07qg43c4fmp/etn021us5r9rhld1vgbh",
                    "storageConfig":{"storageSizeLimit":107374182400}
                })",
            NYql::TDatabaseResolverResponse::TDatabaseDescription{
                TString{"u-lb.etn021us5r9rhld1vgbh.ydb.mdb.yandexcloud.net:2135"},
                TString{"u-lb.etn021us5r9rhld1vgbh.ydb.mdb.yandexcloud.net"},
                2135,
                TString("/ru-central1/b1g7jdjqd07qg43c4fmp/etn021us5r9rhld1vgbh"),
                true
                },
                {}
            );
    }

    Y_UNIT_TEST(ClickHouseNative) {
        Test(
            NYql::EDatabaseType::ClickHouse,
            NYql::NConnector::NApi::EProtocol::NATIVE,
            "https://mdb.api.cloud.yandex.net:443/managed-clickhouse/v1/clusters/etn021us5r9rhld1vgbh/hosts",
            "200",
            R"({
                "hosts": [
                {
                "services": [
                    {
                    "type": "CLICKHOUSE",
                    "health": "ALIVE"
                    }
                ],
                "name": "rc1a-d6dv17lv47v5mcop.mdb.yandexcloud.net",
                "clusterId": "c9ql09h4firghvrv49jt",
                "zoneId": "ru-central1-a",
                "type": "CLICKHOUSE",
                "health": "ALIVE"
                }
                ]
                })",
            NYql::TDatabaseResolverResponse::TDatabaseDescription{
                TString{""},
                TString{"rc1a-d6dv17lv47v5mcop.db.yandex.net"},
                9440,
                TString(""),
                true
                },
                {}
            );
    }

    Y_UNIT_TEST(ClickHouseHttp) {
        Test(
            NYql::EDatabaseType::ClickHouse,
            NYql::NConnector::NApi::EProtocol::HTTP,
            "https://mdb.api.cloud.yandex.net:443/managed-clickhouse/v1/clusters/etn021us5r9rhld1vgbh/hosts",
            "200",
            R"({
                "hosts": [
                {
                "services": [
                    {
                    "type": "CLICKHOUSE",
                    "health": "ALIVE"
                    }
                ],
                "name": "rc1a-d6dv17lv47v5mcop.mdb.yandexcloud.net",
                "clusterId": "c9ql09h4firghvrv49jt",
                "zoneId": "ru-central1-a",
                "type": "CLICKHOUSE",
                "health": "ALIVE"
                }
                ]
                })",
            NYql::TDatabaseResolverResponse::TDatabaseDescription{
                TString{""},
                TString{"rc1a-d6dv17lv47v5mcop.db.yandex.net"},
                8443,
                TString(""),
                true
                },
                {}
            );
    }

    Y_UNIT_TEST(ClickHouse_PermissionDenied) {
        NYql::TIssues issues{
            NYql::TIssue(
                TStringBuilder{} << MakeErrorPrefix(
                    "mdb.api.cloud.yandex.net:443",
                    "/managed-clickhouse/v1/clusters/etn021us5r9rhld1vgbh/hosts",
                    "etn021us5r9rhld1vgbh",
                    NYql::EDatabaseType::ClickHouse
                ) << NoPermissionStr << " Please check that your service account has role `managed-clickhouse.viewer`."
            )
        };

        Test(
            NYql::EDatabaseType::ClickHouse,
            NYql::NConnector::NApi::EProtocol::HTTP,
            "https://mdb.api.cloud.yandex.net:443/managed-clickhouse/v1/clusters/etn021us5r9rhld1vgbh/hosts",
            "403",
            R"(
                {
                    "code": 7,
                    "message": "Permission denied",
                    "details": [
                        {
                            "@type": "type.googleapis.com/google.rpc.RequestInfo",
                            "requestId": "a943c092-d596-4e0e-ae7b-1f67f9d8164e"
                        }
                    ]
                }
            )",
            NYql::TDatabaseResolverResponse::TDatabaseDescription{
                },
                issues
            );
    }

    Y_UNIT_TEST(PostgreSQL) {
        Test(
            NYql::EDatabaseType::PostgreSQL,
            NYql::NConnector::NApi::EProtocol::NATIVE,
            "https://mdb.api.cloud.yandex.net:443/managed-postgresql/v1/clusters/etn021us5r9rhld1vgbh/hosts",
            "200",
            R"({
                "hosts": [
                {
                "services": [
                    {
                    "type": "POOLER",
                    "health": "ALIVE"
                    },
                    {
                    "type": "POSTGRESQL",
                    "health": "ALIVE"
                    }
                ],
                "name": "rc1b-eyt6dtobu96rwydq.mdb.yandexcloud.net",
                "clusterId": "c9qb2bjghs8onbncpamk",
                "zoneId": "ru-central1-b",
                "role": "MASTER",
                "health": "ALIVE"
                }
                ]
                })",
            NYql::TDatabaseResolverResponse::TDatabaseDescription{
                TString{""},
                TString{"rc1b-eyt6dtobu96rwydq.db.yandex.net"},
                6432,
                TString(""),
                true
                },
                {}
            );
    }

    Y_UNIT_TEST(PostgreSQL_PermissionDenied) {
        NYql::TIssues issues{
            NYql::TIssue(
                TStringBuilder{} << MakeErrorPrefix(
                    "mdb.api.cloud.yandex.net:443",
                    "/managed-postgresql/v1/clusters/etn021us5r9rhld1vgbh/hosts",
                    "etn021us5r9rhld1vgbh",
                    NYql::EDatabaseType::PostgreSQL
                ) << NoPermissionStr << " Please check that your service account has role `managed-postgresql.viewer`."
            )
        };

        Test(
            NYql::EDatabaseType::PostgreSQL,
            NYql::NConnector::NApi::EProtocol::NATIVE,
            "https://mdb.api.cloud.yandex.net:443/managed-postgresql/v1/clusters/etn021us5r9rhld1vgbh/hosts",
            "403",
            R"(
                {
                    "code": 7,
                    "message": "Permission denied",
                    "details": [
                        {
                            "@type": "type.googleapis.com/google.rpc.RequestInfo",
                            "requestId": "a943c092-d596-4e0e-ae7b-1f67f9d8164e"
                        }
                    ]
                }
            )",
            NYql::TDatabaseResolverResponse::TDatabaseDescription{
                },
                issues
            );
    }

    Y_UNIT_TEST(Greenplum_MasterNode) {
        Test(
            NYql::EDatabaseType::Greenplum,
            NYql::NConnector::NApi::EProtocol::NATIVE,
            "https://mdb.api.cloud.yandex.net:443/managed-greenplum/v1/clusters/etn021us5r9rhld1vgbh/master-hosts",
            "200",
            R"({
                "hosts": [
                {
                 "resources": {
                 "resourcePresetId": "s3-c8-m32",
                 "diskSize": "395136991232",
                "diskTypeId": "local-ssd"
                },
                "assignPublicIp": false,
                "name": "rc1d-51jc89m9q72vcdkn.mdb.yandexcloud.net",
                "clusterId": "c9qfrvbs21vo0a56s5hm",
                "zoneId": "ru-central1-d",
                "type": "MASTER",
                "health": "ALIVE",
                "subnetId": "fl8vtt2td9qbtlqdj5ji"
                }
                ]
            })",
        NYql::TDatabaseResolverResponse::TDatabaseDescription{
            TString{""},
                    TString{"rc1d-51jc89m9q72vcdkn.db.yandex.net"},
                    6432,
                    TString(""),
                    true},
                {});
    }

    Y_UNIT_TEST(Greenplum_PermissionDenied) {
            NYql::TIssues issues{
                NYql::TIssue(
                    TStringBuilder{} << MakeErrorPrefix(
                                            "mdb.api.cloud.yandex.net:443",
                                            "/managed-greenplum/v1/clusters/etn021us5r9rhld1vgbh/master-hosts",
                                            "etn021us5r9rhld1vgbh",
                                            NYql::EDatabaseType::Greenplum)
                                     << NoPermissionStr)};

            Test(
                NYql::EDatabaseType::Greenplum,
                NYql::NConnector::NApi::EProtocol::NATIVE,
                "https://mdb.api.cloud.yandex.net:443/managed-greenplum/v1/clusters/etn021us5r9rhld1vgbh/master-hosts",
                "403",
                R"(
                {
                    "code": 7,
                    "message": "Permission denied",
                    "details": [
                        {
                            "@type": "type.googleapis.com/google.rpc.RequestInfo",
                            "requestId": "a943c092-d596-4e0e-ae7b-1f67f9d8164e"
                        }
                    ]
                }
            )",
                NYql::TDatabaseResolverResponse::TDatabaseDescription{},
                issues);
    }

    Y_UNIT_TEST(MySQL) {
        Test(
            NYql::EDatabaseType::MySQL,
            NYql::NConnector::NApi::EProtocol::NATIVE,
            "https://mdb.api.cloud.yandex.net:443/managed-mysql/v1/clusters/etn021us5r9rhld1vgbh/hosts",
            "200",
            R"({
                "hosts": [
                {
                "services": [
                    {
                    "type": "POOLER",
                    "health": "ALIVE"
                    },
                    {
                    "type": "MYSQL",
                    "health": "ALIVE"
                    }
                ],
                "name": "rc1b-eyt6dtobu96rwydq.mdb.yandexcloud.net",
                "clusterId": "c9qb2bjghs8onbncpamk",
                "zoneId": "ru-central1-b",
                "role": "MASTER",
                "health": "ALIVE"
                }
                ]
                })",
            NYql::TDatabaseResolverResponse::TDatabaseDescription{
                TString{""},
                TString{"rc1b-eyt6dtobu96rwydq.db.yandex.net"},
                3306,
                TString(""),
                true
                },
                {});
    }

    Y_UNIT_TEST(MySQL_PermissionDenied) {
        NYql::TIssues issues{
            NYql::TIssue(
                TStringBuilder{} << MakeErrorPrefix(
                    "mdb.api.cloud.yandex.net:443",
                    "/managed-mysql/v1/clusters/etn021us5r9rhld1vgbh/hosts",
                    "etn021us5r9rhld1vgbh",
                    NYql::EDatabaseType::MySQL
                ) << NoPermissionStr
            )
        };

        Test(
            NYql::EDatabaseType::MySQL,
            NYql::NConnector::NApi::EProtocol::NATIVE,
            "https://mdb.api.cloud.yandex.net:443/managed-mysql/v1/clusters/etn021us5r9rhld1vgbh/hosts",
            "403",
            R"(
                {
                    "code": 7,
                    "message": "Permission denied",
                    "details": [
                        {
                            "@type": "type.googleapis.com/google.rpc.RequestInfo",
                            "requestId": "a943c092-d596-4e0e-ae7b-1f67f9d8164e"
                        }
                    ]
                }
            )",
            NYql::TDatabaseResolverResponse::TDatabaseDescription{},
                issues
            );
    }
    

    Y_UNIT_TEST(DataStreams_PermissionDenied) {
        NYql::TIssues issues{
            NYql::TIssue(
                TStringBuilder{} << MakeErrorPrefix(
                    "ydbc.ydb.cloud.yandex.net:8789",
                    "/ydbc/cloud-prod/database?databaseId=etn021us5r9rhld1vgbh",
                    "etn021us5r9rhld1vgbh",
                    NYql::EDatabaseType::DataStreams
                ) << NoPermissionStr 
            )
        };
        Test(
            NYql::EDatabaseType::DataStreams,
            NYql::NConnector::NApi::EProtocol::PROTOCOL_UNSPECIFIED,
            "https://ydbc.ydb.cloud.yandex.net:8789/ydbc/cloud-prod/database?databaseId=etn021us5r9rhld1vgbh",
            "403",
            R"(
                {
                    "message": "Permission denied"
                })",
            NYql::TDatabaseResolverResponse::TDatabaseDescription{
                },
                issues
            );
    }

    Y_UNIT_TEST(ResolveTwoDataStreamsFirstError) {
       TTestBootstrap bootstrap;

        NYql::TDatabaseAuth databaseAuth;
        databaseAuth.UseTls = true;
        databaseAuth.Protocol = NYql::NConnector::NApi::EProtocol::PROTOCOL_UNSPECIFIED;

        TString databaseId1{"etn021us5r9rhld1vgb1"};
        TString databaseId2{"etn021us5r9rhld1vgb2"};
        auto requestIdAndDatabaseType1 = std::make_pair(databaseId1, NYql::EDatabaseType::DataStreams);
        auto requestIdAndDatabaseType2 = std::make_pair(databaseId2, NYql::EDatabaseType::DataStreams);

        bootstrap.Send(new IEventHandle(
            bootstrap.DatabaseResolver,
            bootstrap.AsyncResolver,
            new NFq::TEvents::TEvEndpointRequest(
                NYql::IDatabaseAsyncResolver::TDatabaseAuthMap({
                    std::make_pair(requestIdAndDatabaseType1, databaseAuth),
                    std::make_pair(requestIdAndDatabaseType2, databaseAuth)}),
                TString("https://ydbc.ydb.cloud.yandex.net:8789/ydbc/cloud-prod"),
                TString("https://mdb.api.cloud.yandex.net:443"),
                TString("traceId"),
                NFq::MakeMdbEndpointGeneratorGeneric(true))));

        auto httpRequest1 = NHttp::THttpOutgoingRequest::CreateRequestGet("https://ydbc.ydb.cloud.yandex.net:8789/ydbc/cloud-prod/database?databaseId=etn021us5r9rhld1vgb1");
        auto httpRequest2 = NHttp::THttpOutgoingRequest::CreateRequestGet("https://ydbc.ydb.cloud.yandex.net:8789/ydbc/cloud-prod/database?databaseId=etn021us5r9rhld1vgb2");

        NHttp::TEvHttpProxy::TEvHttpOutgoingRequest::TPtr httpOutgoingRequestHolder1 = bootstrap.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(bootstrap.HttpProxy, TDuration::Seconds(10));
        NHttp::TEvHttpProxy::TEvHttpOutgoingRequest::TPtr httpOutgoingRequestHolder2 = bootstrap.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(bootstrap.HttpProxy, TDuration::Seconds(10));
        NHttp::TEvHttpProxy::TEvHttpOutgoingRequest* httpOutgoingRequest1 = httpOutgoingRequestHolder1.Get()->Get();
        NHttp::TEvHttpProxy::TEvHttpOutgoingRequest* httpOutgoingRequest2 = httpOutgoingRequestHolder2.Get()->Get();
        if (httpOutgoingRequest1->Request->URL != httpRequest1->URL) {
            std::swap(httpOutgoingRequest1, httpOutgoingRequest2);
        }

        NActors::TActorId processorActorId = httpOutgoingRequestHolder1->Sender;
        bootstrap.WaitForBootstrap();

        auto response1 = std::make_unique<NHttp::THttpIncomingResponse>(nullptr);
        response1->Status = "404";
        response1->Body = R"({"message":"Database not found"})";

        bootstrap.Send(new IEventHandle(
            processorActorId,
            bootstrap.HttpProxy,
            new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(httpOutgoingRequest1->Request, response1.release(), "")));

        auto response2 = std::make_unique<NHttp::THttpIncomingResponse>(nullptr);
        response2->Status = "200";
        response2->Body = R"({"endpoint":"grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1g7jdjqd07qg43c4fmp/etn021us5r9rhld1vgbh"})";

        bootstrap.Send(new IEventHandle(
            processorActorId,
            bootstrap.HttpProxy,
            new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(httpOutgoingRequest2->Request, response2.release(), "")));

        NYql::TDatabaseResolverResponse::TDatabaseDescriptionMap result;
        result[requestIdAndDatabaseType2] = NYql::TDatabaseResolverResponse::TDatabaseDescription{
                TString{"yds.serverless.yandexcloud.net:2135"},
                TString{"yds.serverless.yandexcloud.net"},
                2135,
                TString("/ru-central1/b1g7jdjqd07qg43c4fmp/etn021us5r9rhld1vgbh"),
                true
            };

        NYql::TIssues issues{
            NYql::TIssue(
                TStringBuilder() << MakeErrorPrefix(
                    "ydbc.ydb.cloud.yandex.net:8789", 
                    "/ydbc/cloud-prod/database?databaseId=etn021us5r9rhld1vgb1", 
                    "etn021us5r9rhld1vgb1", 
                    NYql::EDatabaseType::DataStreams)<< "\nStatus: 404\nResponse body: {\"message\":\"Database not found\"}"
            )
        };

        bootstrap.ExpectEvent<TEvents::TEvEndpointResponse>(bootstrap.AsyncResolver, 
            NFq::TEvents::TEvEndpointResponse(
                NYql::TDatabaseResolverResponse(std::move(result), false, issues)));
    }

}

} // namespace NFq
