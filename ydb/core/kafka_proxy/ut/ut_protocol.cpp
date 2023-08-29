#include <library/cpp/testing/unittest/registar.h>

#include "../kafka_messages.h"

#include <ydb/services/ydb/ydb_common_ut.h>
#include <ydb/services/ydb/ydb_keys_ut.h>

#include <ydb/public/sdk/cpp/client/ydb_datastreams/datastreams.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/api/grpc/draft/ydb_datastreams_v1.grpc.pb.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/digest/md5/md5.h>

#include <util/system/tempfile.h>

#include <random>


using namespace NKafka;
using namespace NYdb;
using namespace NYdb::NTable;

static constexpr const char NON_CHARGEABLE_USER[] = "superuser@builtin";
static constexpr const char NON_CHARGEABLE_USER_X[] = "superuser_x@builtin";
static constexpr const char NON_CHARGEABLE_USER_Y[] = "superuser_y@builtin";

static constexpr const char DEFAULT_CLOUD_ID[] = "somecloud";
static constexpr const char DEFAULT_FOLDER_ID[] = "somefolder";

struct WithSslAndAuth : TKikimrTestSettings {
    static constexpr bool SSL = true;
    static constexpr bool AUTH = true;
};
using TKikimrWithGrpcAndRootSchemaSecure = NYdb::TBasicKikimrWithGrpcAndRootSchema<WithSslAndAuth>;

char Hex0(const unsigned char c) {
    return c < 10 ? '0' + c : 'A' + c - 10;
}

void Print(const TBuffer& buffer) {
    TStringBuilder sb;
    for (size_t i = 0; i < buffer.Size(); ++i) {
        char c = buffer.Data()[i];
        if (i > 0) {
            sb << ", ";
        }
        sb << "0x" << Hex0((c & 0xF0) >> 4) << Hex0(c & 0x0F);
    }
    Cerr << ">>>>> Packet sent: " << sb << Endl;
}


template<class TKikimr, bool secure>
class TTestServer {
public:
    TIpPort Port;

    TTestServer() {
        TPortManager portManager;
        Port = portManager.GetTcpPort();

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableAuthConfig()->SetUseLoginProvider(true);
        appConfig.MutableAuthConfig()->SetUseBlackBox(false);
        appConfig.MutablePQConfig()->SetTopicsAreFirstClassCitizen(true);
        appConfig.MutablePQConfig()->SetEnabled(true);
        // NOTE(shmel1k@): KIKIMR-14221
        appConfig.MutablePQConfig()->SetCheckACL(false);
        appConfig.MutablePQConfig()->SetRequireCredentialsInNewProtocol(false);

        auto cst = appConfig.MutablePQConfig()->AddClientServiceType();
        cst->SetName("data-transfer");
        cst = appConfig.MutablePQConfig()->AddClientServiceType();
        cst->SetName("data-transfer2");

        appConfig.MutableKafkaProxyConfig()->SetEnableKafkaProxy(true);
        appConfig.MutableKafkaProxyConfig()->SetListeningPort(Port);
        appConfig.MutableKafkaProxyConfig()->SetMaxMessageSize(1024);
        appConfig.MutableKafkaProxyConfig()->SetMaxInflightSize(2048);


        appConfig.MutablePQConfig()->MutableQuotingConfig()->SetEnableQuoting(true);
        appConfig.MutablePQConfig()->MutableQuotingConfig()->SetQuotaWaitDurationMs(300);
        appConfig.MutablePQConfig()->MutableQuotingConfig()->SetPartitionReadQuotaIsTwiceWriteQuota(true);
        appConfig.MutablePQConfig()->MutableBillingMeteringConfig()->SetEnabled(true);
        appConfig.MutablePQConfig()->MutableBillingMeteringConfig()->SetFlushIntervalSec(1);
        appConfig.MutablePQConfig()->AddClientServiceType()->SetName("data-streams");
        appConfig.MutablePQConfig()->AddNonChargeableUser(NON_CHARGEABLE_USER);
        appConfig.MutablePQConfig()->AddNonChargeableUser(NON_CHARGEABLE_USER_X);
        appConfig.MutablePQConfig()->AddNonChargeableUser(NON_CHARGEABLE_USER_Y);

        appConfig.MutablePQConfig()->AddValidWriteSpeedLimitsKbPerSec(128);
        appConfig.MutablePQConfig()->AddValidWriteSpeedLimitsKbPerSec(512);
        appConfig.MutablePQConfig()->AddValidWriteSpeedLimitsKbPerSec(1_KB);

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

        MeteringFile = MakeHolder<TTempFileHandle>();
        appConfig.MutableMeteringConfig()->SetMeteringFilePath(MeteringFile->Name());

        if (secure) {
            appConfig.MutablePQConfig()->SetRequireCredentialsInNewProtocol(true);
        }
        KikimrServer = std::unique_ptr<TKikimr>(new TKikimr(
            std::move(appConfig),
            {},
            {},
            false,
            nullptr,
            nullptr,
            0)
        );
        KikimrServer->GetRuntime()->SetLogPriority(NKikimrServices::KAFKA_PROXY, NActors::NLog::PRI_TRACE);

        ui16 grpc = KikimrServer->GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto driverConfig = TDriverConfig().SetEndpoint(location).SetLog(CreateLogBackend("cerr", TLOG_DEBUG));
        if (secure) {
            driverConfig.UseSecureConnection(TString(NYdbSslTestData::CaCrt));
        } else {
            driverConfig.SetDatabase("/Root/");
        }

        Driver = std::make_unique<TDriver>(std::move(driverConfig));

        {
            NYdb::NScheme::TSchemeClient schemeClient(*Driver);
            NYdb::NScheme::TPermissions permissions("user@builtin", {"ydb.generic.read", "ydb.generic.write"});

            auto result = schemeClient.ModifyPermissions("/Root",
                NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(permissions)
            ).ExtractValueSync();
            Cerr << result.GetIssues().ToString() << "\n";
            UNIT_ASSERT(result.IsSuccess());
        }

        TClient client(*(KikimrServer->ServerSettings));
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 client.AlterUserAttributes("/", "Root", {{"folder_id", DEFAULT_FOLDER_ID},
                                                                          {"cloud_id", DEFAULT_CLOUD_ID},
                                                                          {"database_id", "root"}}));

        auto status = client.CreateUser("/Root", "ouruser", "ourUserPassword");
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::MSTATUS_OK);
    }

public:
    std::unique_ptr<TKikimr> KikimrServer;
    std::unique_ptr<TDriver> Driver;
    THolder<TTempFileHandle> MeteringFile;
};

using TInsecureTestServer = TTestServer<TKikimrWithGrpcAndRootSchema, false>;
using TSecureTestServer = TTestServer<TKikimrWithGrpcAndRootSchemaSecure, true>;

void Write(TSocketOutput& so, TApiMessage* request, TKafkaVersion version) {
    TWritableBuf sb(nullptr, request->Size(version) + 1000);
    TKafkaWritable writable(sb);
    request->Write(writable, version);
    so.Write(sb.Data(), sb.Size());

    Print(sb.GetBuffer());
}

void Write(TSocketOutput& so, TRequestHeaderData* header, TApiMessage* request) {
    TKafkaVersion version = header->RequestApiVersion;
    TKafkaVersion headerVersion = RequestHeaderVersion(request->ApiKey(), version);

    TKafkaInt32 size = header->Size(headerVersion) + request->Size(version);
    Cerr << ">>>>> Size=" << size << Endl;
    NKafka::NormalizeNumber(size);
    so.Write(&size, sizeof(size));

    Write(so, header, headerVersion);
    Write(so, request, version);

    so.Flush();
}

std::unique_ptr<TApiMessage> Read(TSocketInput& si, TRequestHeaderData* requestHeader) {
    TKafkaInt32 size;

    si.Read(&size, sizeof(size));
    NKafka::NormalizeNumber(size);

    TBuffer buffer;
    buffer.Resize(size);
    si.Load(buffer.Data(), size);

    TKafkaVersion headerVersion = ResponseHeaderVersion(requestHeader->RequestApiKey, requestHeader->RequestApiVersion);

    TKafkaReadable readable(buffer);

    TResponseHeaderData header;
    header.Read(readable, headerVersion);

    UNIT_ASSERT_VALUES_EQUAL(header.CorrelationId, requestHeader->CorrelationId);

    auto response = CreateResponse(requestHeader->RequestApiKey);
    response->Read(readable, requestHeader->RequestApiVersion);

    return response;
}

Y_UNIT_TEST_SUITE(KafkaProtocol) {
    Y_UNIT_TEST(ProduceScenario) {
        TInsecureTestServer testServer;

        TString topicName = "/Root/topic-0-test";

        {
            NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
            auto result = pqClient.CreateTopic(topicName).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        TNetworkAddress addr("localhost", testServer.Port);
        TSocket s(addr);
        TSocketOutput so(s);
        TSocketInput si(s);

        size_t correlationId = 0;

        {
            Cerr << ">>>>> ApiVersionsRequest\n";

            TRequestHeaderData header;
            header.RequestApiKey = NKafka::EApiKey::API_VERSIONS;
            header.RequestApiVersion = 2;
            header.CorrelationId = correlationId++;
            header.ClientId = "test";

            TApiVersionsRequestData request;
            request.ClientSoftwareName = "SuperTest";
            request.ClientSoftwareVersion = "3100.7.13";

            Write(so, &header, &request);

            auto response = Read(si, &header);
            auto* msg = dynamic_cast<TApiVersionsResponseData*>(response.get());

            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(msg->ApiKeys.size(), 6u);
        }

        {
            Cerr << ">>>>> SaslHandshakeRequest\n";

            TRequestHeaderData header;
            header.RequestApiKey = NKafka::EApiKey::SASL_HANDSHAKE;
            header.RequestApiVersion = 1;
            header.CorrelationId = correlationId++;
            header.ClientId = "test";

            TSaslHandshakeRequestData request;
            request.Mechanism = "PLAIN";

            Write(so, &header, &request);

            auto response = Read(si, &header);
            auto* msg = dynamic_cast<TSaslHandshakeResponseData*>(response.get());

            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(msg->Mechanisms.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(*msg->Mechanisms[0], "PLAIN");
        }

        {
            Cerr << ">>>>> SaslAuthenticateRequestData\n";
            char authBytes[] = "ignored\0ouruser@/Root\0ourUserPassword";

            TRequestHeaderData header;
            header.RequestApiKey = NKafka::EApiKey::SASL_AUTHENTICATE;
            header.RequestApiVersion = 2;
            header.CorrelationId = correlationId++;
            header.ClientId = "test";

            TSaslAuthenticateRequestData request;
            request.AuthBytes = TKafkaRawBytes(authBytes, sizeof(authBytes) - 1);

            Write(so, &header, &request);

            auto response = Read(si, &header);
            auto* msg = dynamic_cast<TSaslAuthenticateResponseData*>(response.get());

            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }

        {
            Cerr << ">>>>> TInitProducerIdRequestData\n";

            TRequestHeaderData header;
            header.RequestApiKey = NKafka::EApiKey::INIT_PRODUCER_ID;
            header.RequestApiVersion = 4;
            header.CorrelationId = correlationId++;
            header.ClientId = "test";

            TInitProducerIdRequestData request;
            request.TransactionTimeoutMs = 5000;

            Write(so, &header, &request);

            auto response = Read(si, &header);
            auto* msg = dynamic_cast<TInitProducerIdResponseData*>(response.get());

            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }

        {
            Cerr << ">>>>> TProduceRequestData\n";

            TRequestHeaderData header;
            header.RequestApiKey = NKafka::EApiKey::PRODUCE;
            header.RequestApiVersion = 9;
            header.CorrelationId = correlationId++;
            header.ClientId = "test-client-random-string";

            TProduceRequestData request;
            request.TopicData.resize(1);
            request.TopicData[0].Name = topicName;
            request.TopicData[0].PartitionData.resize(1);
            request.TopicData[0].PartitionData[0].Index = 0; // Partition id
            request.TopicData[0].PartitionData[0].Records.emplace();
            request.TopicData[0].PartitionData[0].Records->BaseOffset = 3;
            request.TopicData[0].PartitionData[0].Records->BaseSequence = 5;
            request.TopicData[0].PartitionData[0].Records->Magic = 2; // Current supported
            request.TopicData[0].PartitionData[0].Records->Records.resize(1);
            request.TopicData[0].PartitionData[0].Records->Records[0].Key = "record-key";
            request.TopicData[0].PartitionData[0].Records->Records[0].Value = "record-value";

            Write(so, &header, &request);

            auto response = Read(si, &header);
            auto* msg = dynamic_cast<TProduceResponseData*>(response.get());

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Name, topicName);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].Index, 0);
           // UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }
    }
}