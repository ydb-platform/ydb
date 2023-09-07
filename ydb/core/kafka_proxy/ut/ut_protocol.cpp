#include <library/cpp/testing/unittest/registar.h>

#include "../kafka_messages.h"

#include <ydb/services/ydb/ydb_common_ut.h>
#include <ydb/services/ydb/ydb_keys_ut.h>

#include <ydb/library/testlib/service_mocks/access_service_mock.h>

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

struct WithSslAndAuth: TKikimrTestSettings {
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

template <class TKikimr, bool secure>
class TTestServer {
public:
    TIpPort Port;

    TTestServer() {
        TPortManager portManager;
        Port = portManager.GetTcpPort();

        ui16 accessServicePort = portManager.GetPort(4284);
        TString accessServiceEndpoint = "localhost:" + ToString(accessServicePort);

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableAuthConfig()->SetUseLoginProvider(true);
        appConfig.MutableAuthConfig()->SetUseBlackBox(false);
        appConfig.MutableAuthConfig()->SetUseBlackBox(false);
        appConfig.MutableAuthConfig()->SetUseAccessService(true);
        appConfig.MutableAuthConfig()->SetUseAccessServiceApiKey(true);
        appConfig.MutableAuthConfig()->SetUseAccessServiceTLS(false);
        appConfig.MutableAuthConfig()->SetAccessServiceEndpoint(accessServiceEndpoint);

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
        KikimrServer = std::unique_ptr<TKikimr>(new TKikimr(std::move(appConfig), {}, {}, false, nullptr, nullptr, 0));
        KikimrServer->GetRuntime()->SetLogPriority(NKikimrServices::KAFKA_PROXY, NActors::NLog::PRI_TRACE);
        KikimrServer->GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        KikimrServer->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        KikimrServer->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_PROXY_NO_CONNECT_ACCESS, NLog::PRI_TRACE);

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

            auto result = schemeClient
                              .ModifyPermissions(
                                  "/Root", NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(permissions))
                              .ExtractValueSync();
            Cerr << result.GetIssues().ToString() << "\n";
            UNIT_ASSERT(result.IsSuccess());
        }

        TClient client(*(KikimrServer->ServerSettings));
        UNIT_ASSERT_VALUES_EQUAL(
            NMsgBusProxy::MSTATUS_OK,
            client.AlterUserAttributes("/", "Root",
                                       {{"folder_id", DEFAULT_FOLDER_ID},
                                        {"cloud_id", DEFAULT_CLOUD_ID},
                                        {"database_id", "root"},
                                        {"serverless_rt_coordination_node_path", "/Coordinator/Root"},
                                        {"serverless_rt_base_resource_ru", "/ru_Root"}}));

        {
            auto status = client.CreateUser("/Root", "ouruser", "ourUserPassword");
            UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::MSTATUS_OK);

            NYdb::NScheme::TSchemeClient schemeClient(*Driver);
            NYdb::NScheme::TPermissions permissions("ouruser", {"ydb.generic.read", "ydb.generic.write"});

            auto result = schemeClient
                              .ModifyPermissions(
                                  "/Root", NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(permissions))
                              .ExtractValueSync();
            Cerr << result.GetIssues().ToString() << "\n";
            UNIT_ASSERT(result.IsSuccess());
        }

        {
            // Access Server Mock
            grpc::ServerBuilder builder;
            builder.AddListeningPort(accessServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&accessServiceMock);
            AccessServer = builder.BuildAndStart();
        }
    }

public:
    std::unique_ptr<TKikimr> KikimrServer;
    std::unique_ptr<TDriver> Driver;
    THolder<TTempFileHandle> MeteringFile;

    TTicketParserAccessServiceMock accessServiceMock;
    std::unique_ptr<grpc::Server> AccessServer;
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

void AssertMessageMeta(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& msg, const TString& field,
                       const TString& expectedValue) {
    if (msg.GetMessageMeta()) {
        for (auto& [k, v] : msg.GetMessageMeta()->Fields) {
            Cerr << ">>>>> key=" << k << ", value=" << v << Endl;
            if (field == k) {
                UNIT_ASSERT_STRINGS_EQUAL(v, expectedValue);
                return;
            }
        }
    }
    UNIT_ASSERT_C(false, "Field " << field << " not found in message meta");
}

std::vector<NTopic::TReadSessionEvent::TDataReceivedEvent> Read(std::shared_ptr<NYdb::NTopic::IReadSession> reader) {
    std::vector<NTopic::TReadSessionEvent::TDataReceivedEvent> result;
    while (true) {
        auto event = reader->GetEvent(true);
        if (!event)
            break;
        if (auto dataEvent = std::get_if<NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
            result.push_back(*dataEvent);
            break;
        } else if (auto* lockEv = std::get_if<NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
            lockEv->Confirm();
        } else if (auto* releaseEv = std::get_if<NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&*event)) {
            releaseEv->Confirm();
        } else if (auto* closeSessionEvent = std::get_if<NTopic::TSessionClosedEvent>(&*event)) {
            break;
        }
    }
    Cerr << ">>>>> Received messages " << result.size() << Endl;
    return result;
}

class TTestClient {
public:
    TTestClient(ui16 port, const TString clientName = "TestClient")
        : Addr("localhost", port)
        , Socket(Addr)
        , So(Socket)
        , Si(Socket)
        , Correlation(0)
        , ClientName(clientName) {
    }

    TApiVersionsResponseData::TPtr ApiVersions() {
        Cerr << ">>>>> ApiVersionsRequest\n";

        TRequestHeaderData header = Header(NKafka::EApiKey::API_VERSIONS, 2);

        TApiVersionsRequestData request;
        request.ClientSoftwareName = "SuperTest";
        request.ClientSoftwareVersion = "3100.7.13";

        return WriteAndRead<TApiVersionsResponseData>(header, request);
    }

    TSaslHandshakeResponseData::TPtr SaslHandshake(const TString& mechanism = "PLAIN") {
        Cerr << ">>>>> SaslHandshakeRequest\n";

        TRequestHeaderData header = Header(NKafka::EApiKey::SASL_HANDSHAKE, 1);

        TSaslHandshakeRequestData request;
        request.Mechanism = mechanism;

        return WriteAndRead<TSaslHandshakeResponseData>(header, request);
    }

    TSaslAuthenticateResponseData::TPtr SaslAuthenticate(const TString& user, const TString& password) {
        Cerr << ">>>>> SaslAuthenticateRequestData\n";

        TStringBuilder authBytes;
        authBytes << "ignored" << '\0' << user << '\0' << password;

        TRequestHeaderData header = Header(NKafka::EApiKey::SASL_AUTHENTICATE, 2);

        TSaslAuthenticateRequestData request;
        request.AuthBytes = TKafkaRawBytes(authBytes.data(), authBytes.size());

        return WriteAndRead<TSaslAuthenticateResponseData>(header, request);
    }

    TInitProducerIdResponseData::TPtr InitProducerId() {
        Cerr << ">>>>> TInitProducerIdRequestData\n";

        TRequestHeaderData header = Header(NKafka::EApiKey::INIT_PRODUCER_ID, 4);

        TInitProducerIdRequestData request;
        request.TransactionTimeoutMs = 5000;

        return WriteAndRead<TInitProducerIdResponseData>(header, request);
    }

    TProduceResponseData::TPtr Produce(const TString& topicName, ui32 partition, const TKafkaRecordBatch& batch) {
        std::vector<std::pair<ui32, TKafkaRecordBatch>> msgs;
        msgs.emplace_back(partition, batch);
        return Produce(topicName, msgs);
    }

    TProduceResponseData::TPtr Produce(const TString& topicName, const std::vector<std::pair<ui32, TKafkaRecordBatch>> msgs) {
        Cerr << ">>>>> TProduceRequestData\n";

        TRequestHeaderData header = Header(NKafka::EApiKey::PRODUCE, 9);

        TProduceRequestData request;
        request.TopicData.resize(1);
        request.TopicData[0].Name = topicName;
        request.TopicData[0].PartitionData.resize(msgs.size());
        for(size_t i = 0 ; i < msgs.size(); ++i) {
            request.TopicData[0].PartitionData[i].Index = msgs[i].first;
            request.TopicData[0].PartitionData[i].Records = msgs[i].second;
        }

        return WriteAndRead<TProduceResponseData>(header, request);
    }

    void UnknownApiKey() {
        Cerr << ">>>>> Unknown apiKey\n";

        TRequestHeaderData header;
        header.RequestApiKey = 7654;
        header.RequestApiVersion = 1;
        header.CorrelationId = NextCorrelation();
        header.ClientId = ClientName;

        TApiVersionsRequestData request;
        request.ClientSoftwareName = "SuperTest";
        request.ClientSoftwareVersion = "3100.7.13";

        Write(So, &header, &request);
    }

protected:
    ui32 NextCorrelation() {
        return Correlation++;
    }

    template <class T>
    typename T::TPtr WriteAndRead(TRequestHeaderData& header, TApiMessage& request) {
        Write(So, &header, &request);

        auto response = Read(Si, &header);
        auto* msg = dynamic_cast<T*>(response.release());
        return std::shared_ptr<T>(msg);
    }

    TRequestHeaderData Header(NKafka::EApiKey apiKey, TKafkaVersion version) {
        TRequestHeaderData header;
        header.RequestApiKey = apiKey;
        header.RequestApiVersion = version;
        header.CorrelationId = NextCorrelation();
        header.ClientId = ClientName;
        return header;
    }

private:
    TNetworkAddress Addr;
    TSocket Socket;
    TSocketOutput So;
    TSocketInput Si;

    ui32 Correlation;
    TString ClientName;
};

Y_UNIT_TEST_SUITE(KafkaProtocol) {
    Y_UNIT_TEST(ProduceScenario) {
        TInsecureTestServer testServer;

        TString topicName = "/Root/topic-0-test";

        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        {
            auto result =
                pqClient
                    .CreateTopic(topicName,
                                 NYdb::NTopic::TCreateTopicSettings()
                                    .PartitioningSettings(10, 100)
                                    .BeginAddConsumer("consumer-0").EndAddConsumer())
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        auto settings = NTopic::TReadSessionSettings()
                            .AppendTopics(NTopic::TTopicReadSettings(topicName))
                            .ConsumerName("consumer-0");
        auto topicReader = pqClient.CreateReadSession(settings);

        TTestClient client(testServer.Port);

        {
            auto msg = client.ApiVersions();

            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(msg->ApiKeys.size(), 6u);
        }

        {
            auto msg = client.SaslHandshake();

            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(msg->Mechanisms.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(*msg->Mechanisms[0], "PLAIN");
        }

        {
            auto msg = client.SaslAuthenticate("ouruser@/Root", "ourUserPassword");

            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }

        {
            auto msg = client.InitProducerId();

            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }

        {
            TString key = "record-key";
            TString value = "record-value";
            TString headerKey = "header-key";
            TString headerValue = "header-value";

            TKafkaRecordBatch batch;
            batch.BaseOffset = 3;
            batch.BaseSequence = 5;
            batch.Magic = 2; // Current supported
            batch.Records.resize(1);
            batch.Records[0].Key = TKafkaRawBytes(key.Data(), key.Size());
            batch.Records[0].Value = TKafkaRawBytes(value.Data(), value.Size());
            batch.Records[0].Headers.resize(1);
            batch.Records[0].Headers[0].Key = TKafkaRawBytes(headerKey.Data(), headerKey.Size());
            batch.Records[0].Headers[0].Value = TKafkaRawBytes(headerValue.Data(), headerValue.Size());

            auto msg = client.Produce(topicName, 0, batch);

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Name, topicName);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].Index, 0);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].ErrorCode,
                                     static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

            auto m = Read(topicReader);
            UNIT_ASSERT_EQUAL(m.size(), 1);

            UNIT_ASSERT_EQUAL(m[0].GetMessages().size(), 1);
            auto& m0 = m[0].GetMessages()[0];
            m0.Commit();

            UNIT_ASSERT_STRINGS_EQUAL(m0.GetData(), value);
            AssertMessageMeta(m0, "__key", key);
            AssertMessageMeta(m0, headerKey, headerValue);
        }

        {
            // Check short topic name

            TKafkaRecordBatch batch;
            batch.BaseOffset = 7;
            batch.BaseSequence = 11;
            batch.Magic = 2; // Current supported
            batch.Records.resize(1);
            batch.Records[0].Key = "record-key-1";
            batch.Records[0].Value = "record-value-1";

            auto msg = client.Produce("topic-0-test", 0, batch);

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Name, "topic-0-test");
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].Index, 0);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].ErrorCode,
                                     static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

            auto m = Read(topicReader);
            UNIT_ASSERT_EQUAL(m.size(), 1);

            UNIT_ASSERT_EQUAL(m[0].GetMessages().size(), 1);
            auto& m0 = m[0].GetMessages()[0];
            m0.Commit();
        }

        {
            // Check for few records

            TKafkaRecordBatch batch;
            batch.BaseOffset = 13;
            batch.BaseSequence = 17;
            batch.Magic = 2; // Current supported
            batch.Records.resize(1);
            batch.Records[0].Key = "record-key-0";
            batch.Records[0].Value = "record-value-0";

            std::vector<std::pair<ui32, TKafkaRecordBatch>> msgs;
            msgs.emplace_back(0, batch);
            msgs.emplace_back(1, batch);

            auto msg = client.Produce("topic-0-test", msgs);

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Name, "topic-0-test");
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].Index, 0);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].ErrorCode,
                                     static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[1].Index, 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[1].ErrorCode,
                                     static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

            {
                auto m = Read(topicReader);
                UNIT_ASSERT_EQUAL(m.size(), 1);

                UNIT_ASSERT_EQUAL(m[0].GetMessages().size(), 1);
                m[0].GetMessages()[0].Commit();
            }

            {
                auto m = Read(topicReader);
                UNIT_ASSERT_EQUAL(m.size(), 1);

                UNIT_ASSERT_EQUAL(m[0].GetMessages().size(), 1);
                m[0].GetMessages()[0].Commit();
            }
        }

        {
            // Unknown topic

            TKafkaRecordBatch batch;
            batch.BaseOffset = 7;
            batch.BaseSequence = 11;
            batch.Magic = 2; // Current supported
            batch.Records.resize(1);
            batch.Records[0].Key = "record-key-1";
            batch.Records[0].Value = "record-value-1";

            auto msg = client.Produce("topic-0-test-not-exists", 0, batch);

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Name, "topic-0-test-not-exists");
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].Index, 0);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].ErrorCode,
                                     static_cast<TKafkaInt16>(EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION));
        }

        {
            // Unknown partition

            TKafkaRecordBatch batch;
            batch.BaseOffset = 7;
            batch.BaseSequence = 11;
            batch.Magic = 2; // Current supported
            batch.Records.resize(1);
            batch.Records[0].Key = "record-key-1";
            batch.Records[0].Value = "record-value-1";

            auto msg = client.Produce("topic-0-test", 10000, batch);

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Name, "topic-0-test");
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].Index, 10000);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].ErrorCode,
                                     static_cast<TKafkaInt16>(EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION));
        }

        {
            // Check unknown ApiKey (must be last. close the session)
            // expect no exception
            client.UnknownApiKey();
        }
    } // Y_UNIT_TEST(ProduceScenario)

    Y_UNIT_TEST(LoginWithApiKey) {
        TInsecureTestServer testServer;

        TString topicName = "/Root/topic-0-test";

        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        {
            auto result =
                pqClient
                    .CreateTopic(topicName,
                                 NYdb::NTopic::TCreateTopicSettings()
                                    .PartitioningSettings(10, 100)
                                    .BeginAddConsumer("consumer-0").EndAddConsumer())
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        auto settings = NTopic::TReadSessionSettings()
                            .AppendTopics(NTopic::TTopicReadSettings(topicName))
                            .ConsumerName("consumer-0");
        auto topicReader = pqClient.CreateReadSession(settings);

        TTestClient client(testServer.Port);

        {
            auto msg = client.ApiVersions();

            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(msg->ApiKeys.size(), 6u);
        }

        {
            auto msg = client.SaslHandshake();

            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(msg->Mechanisms.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(*msg->Mechanisms[0], "PLAIN");
        }

        {
            auto msg = client.SaslAuthenticate("@/Root", "ApiKey-value-valid");

            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }

        Sleep(TDuration::Seconds(1));
    }
} // Y_UNIT_TEST_SUITE(KafkaProtocol)
