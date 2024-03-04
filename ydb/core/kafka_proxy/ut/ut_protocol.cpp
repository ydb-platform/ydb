#include <library/cpp/testing/unittest/registar.h>

#include "../kafka_messages.h"
#include "../actors/actors.h"

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

static constexpr const ui64 FirstTopicOffset = -2;
static constexpr const ui64 LastTopicOffset = -1;

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

    TTestServer(const TString& kafkaApiMode = "1") {
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
                                        {"kafka_api", kafkaApiMode},
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

template<std::derived_from<TApiMessage> T>
TMessagePtr<T> Read(TSocketInput& si, TRequestHeaderData* requestHeader) {
    TKafkaInt32 size;

    si.Read(&size, sizeof(size));
    NKafka::NormalizeNumber(size);

    auto buffer= std::make_shared<TBuffer>();
    buffer->Resize(size);
    si.Load(buffer->Data(), size);

    TKafkaVersion headerVersion = ResponseHeaderVersion(requestHeader->RequestApiKey, requestHeader->RequestApiVersion);

    TKafkaReadable readable(*buffer);

    TResponseHeaderData header;
    header.Read(readable, headerVersion);

    UNIT_ASSERT_VALUES_EQUAL(header.CorrelationId, requestHeader->CorrelationId);

    auto response = CreateResponse(requestHeader->RequestApiKey);
    response->Read(readable, requestHeader->RequestApiVersion);

    return TMessagePtr<T>(buffer, std::shared_ptr<TApiMessage>(response.release()));
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

    TMessagePtr<TApiVersionsResponseData> ApiVersions() {
        Cerr << ">>>>> ApiVersionsRequest\n";

        TRequestHeaderData header = Header(NKafka::EApiKey::API_VERSIONS, 2);

        TApiVersionsRequestData request;
        request.ClientSoftwareName = "SuperTest";
        request.ClientSoftwareVersion = "3100.7.13";

        return WriteAndRead<TApiVersionsResponseData>(header, request);
    }

    TMessagePtr<TSaslHandshakeResponseData> SaslHandshake(const TString& mechanism = "PLAIN") {
        Cerr << ">>>>> SaslHandshakeRequest\n";

        TRequestHeaderData header = Header(NKafka::EApiKey::SASL_HANDSHAKE, 1);

        TSaslHandshakeRequestData request;
        request.Mechanism = mechanism;

        return WriteAndRead<TSaslHandshakeResponseData>(header, request);
    }

    TMessagePtr<TSaslAuthenticateResponseData> SaslAuthenticate(const TString& user, const TString& password) {
        Cerr << ">>>>> SaslAuthenticateRequestData\n";

        TStringBuilder authBytes;
        authBytes << "ignored" << '\0' << user << '\0' << password;

        TRequestHeaderData header = Header(NKafka::EApiKey::SASL_AUTHENTICATE, 2);

        TSaslAuthenticateRequestData request;
        request.AuthBytes = TKafkaRawBytes(authBytes.data(), authBytes.size());

        return WriteAndRead<TSaslAuthenticateResponseData>(header, request);
    }

    TMessagePtr<TInitProducerIdResponseData> InitProducerId() {
        Cerr << ">>>>> TInitProducerIdRequestData\n";

        TRequestHeaderData header = Header(NKafka::EApiKey::INIT_PRODUCER_ID, 4);

        TInitProducerIdRequestData request;
        request.TransactionTimeoutMs = 5000;

        return WriteAndRead<TInitProducerIdResponseData>(header, request);
    }

    TMessagePtr<TOffsetCommitResponseData> OffsetCommit(TString groupId, std::unordered_map<TString, std::vector<std::pair<ui64,ui64>>> topicsToPartions) {
        Cerr << ">>>>> TOffsetCommitRequestData\n";
        
        TRequestHeaderData header = Header(NKafka::EApiKey::OFFSET_COMMIT, 1);

        TOffsetCommitRequestData request;
        request.GroupId = groupId;

        for (const auto& topicToPartitions : topicsToPartions) {
            NKafka::TOffsetCommitRequestData::TOffsetCommitRequestTopic topic;
            topic.Name = topicToPartitions.first;
            
            for (auto partitionAndOffset : topicToPartitions.second) {
                NKafka::TOffsetCommitRequestData::TOffsetCommitRequestTopic::TOffsetCommitRequestPartition partition;
                partition.PartitionIndex = partitionAndOffset.first;
                partition.CommittedOffset = partitionAndOffset.second;
                topic.Partitions.push_back(partition);
            }
            request.Topics.push_back(topic);
        }

        return WriteAndRead<TOffsetCommitResponseData>(header, request);
    }

    TMessagePtr<TProduceResponseData> Produce(const TString& topicName, ui32 partition, const TKafkaRecordBatch& batch) {
        std::vector<std::pair<ui32, TKafkaRecordBatch>> msgs;
        msgs.emplace_back(partition, batch);
        return Produce(topicName, msgs);
    }

    TMessagePtr<TProduceResponseData> Produce(const TString& topicName, const std::vector<std::pair<ui32, TKafkaRecordBatch>> msgs) {
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

    TMessagePtr<TListOffsetsResponseData> ListOffsets(std::vector<std::pair<i32,i64>>& partitions, const TString& topic) {
        Cerr << ">>>>> TListOffsetsRequestData\n";

        TRequestHeaderData header = Header(NKafka::EApiKey::LIST_OFFSETS, 4);

        TListOffsetsRequestData request;
        request.IsolationLevel = 0;
        request.ReplicaId = 0;
        NKafka::TListOffsetsRequestData::TListOffsetsTopic newTopic{};
        newTopic.Name = topic;
        for(auto partition: partitions) {
            NKafka::TListOffsetsRequestData::TListOffsetsTopic::TListOffsetsPartition newPartition{};
            newPartition.PartitionIndex = partition.first;
            newPartition.Timestamp = partition.second;
            newTopic.Partitions.emplace_back(newPartition);
        }
        request.Topics.emplace_back(newTopic);
        return WriteAndRead<TListOffsetsResponseData>(header, request);
    }

    TMessagePtr<TJoinGroupResponseData> JoinGroup(std::vector<TString>& topics, TString& groupId, i32 heartbeatTimeout = 1000000) {
        Cerr << ">>>>> TJoinGroupRequestData\n";

        TRequestHeaderData header = Header(NKafka::EApiKey::JOIN_GROUP, 9);

        TJoinGroupRequestData request;
        request.GroupId = groupId;
        request.ProtocolType = "consumer";
        request.SessionTimeoutMs = heartbeatTimeout;

        NKafka::TJoinGroupRequestData::TJoinGroupRequestProtocol protocol;
        protocol.Name = "roundrobin";

        TConsumerProtocolSubscription subscribtion;

        for (auto& topic: topics) {
            subscribtion.Topics.push_back(topic);
        }

        TKafkaVersion version = 3;

        TWritableBuf buf(nullptr, subscribtion.Size(version) + sizeof(version));
        TKafkaWritable writable(buf);
        writable << version;
        subscribtion.Write(writable, version);

        protocol.Metadata = TKafkaRawBytes(buf.GetBuffer().data(), buf.GetBuffer().size());

        request.Protocols.push_back(protocol);
        return WriteAndRead<TJoinGroupResponseData>(header, request);
    }

    TMessagePtr<TSyncGroupResponseData> SyncGroup(TString& memberId, ui64 generationId, TString& groupId) {
        Cerr << ">>>>> TSyncGroupRequestData\n";

        TRequestHeaderData header = Header(NKafka::EApiKey::SYNC_GROUP, 5);

        TSyncGroupRequestData request;
        request.GroupId = groupId;
        request.ProtocolType = "consumer";
        request.ProtocolName = "roundrobin";
        request.GenerationId = generationId;
        request.GroupId = groupId;
        request.MemberId = memberId;

        return WriteAndRead<TSyncGroupResponseData>(header, request);
    }

    struct TReadInfo {
        std::vector<TConsumerProtocolAssignment::TopicPartition> Partitions;
        TString MemberId;
        i32 GenerationId;
    };

    TReadInfo JoinAndSyncGroup(std::vector<TString>& topics, TString& groupId, i32 heartbeatTimeout = 1000000) {
        auto joinResponse = JoinGroup(topics, groupId, heartbeatTimeout);
        auto memberId = joinResponse->MemberId;
        auto generationId =  joinResponse->GenerationId;
        auto balanceStrategy =  joinResponse->ProtocolName;
        UNIT_ASSERT_VALUES_EQUAL(joinResponse->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
    

        auto syncResponse = SyncGroup(memberId.value(), generationId, groupId);
        UNIT_ASSERT_VALUES_EQUAL(syncResponse->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

        TReadInfo readInfo;
        readInfo.GenerationId = generationId;
        readInfo.MemberId = memberId.value();
        readInfo.Partitions = syncResponse->Assignment.AssignedPartitions;

        return readInfo;
    }

    TMessagePtr<THeartbeatResponseData> Heartbeat(TString& memberId, ui64 generationId, TString& groupId) {
        Cerr << ">>>>> THeartbeatRequestData\n";

        TRequestHeaderData header = Header(NKafka::EApiKey::HEARTBEAT, 4);

        THeartbeatRequestData request;
        request.GroupId = groupId;
        request.MemberId = memberId;
        request.GenerationId = generationId;

        return WriteAndRead<THeartbeatResponseData>(header, request);
    }

    void WaitRebalance(TString& memberId, ui64 generationId, TString& groupId) {
        TKafkaInt16 heartbeatStatus;
        do {
            heartbeatStatus = Heartbeat(memberId, generationId, groupId)->ErrorCode;
        } while (heartbeatStatus == static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

        UNIT_ASSERT_VALUES_EQUAL(heartbeatStatus, static_cast<TKafkaInt16>(EKafkaErrors::REBALANCE_IN_PROGRESS));
    }

    TReadInfo JoinAndSyncGroupAndWaitPartitions(std::vector<TString>& topics, TString& groupId, ui32 expectedPartitionsCount) {
        TReadInfo readInfo;
        for (;;) {
            readInfo = JoinAndSyncGroup(topics, groupId);
            ui32 partitionsCount = 0;
            for (auto topicPartitions: readInfo.Partitions) {
                partitionsCount += topicPartitions.Partitions.size();
            }

            if (partitionsCount == expectedPartitionsCount) {
                break;
            }
            WaitRebalance(readInfo.MemberId, readInfo.GenerationId, groupId);    
        }
        return readInfo;
    }

    TMessagePtr<TLeaveGroupResponseData> LeaveGroup(TString& memberId, TString& groupId) {
        Cerr << ">>>>> TLeaveGroupRequestData\n";

        TRequestHeaderData header = Header(NKafka::EApiKey::LEAVE_GROUP, 5);

        TLeaveGroupRequestData request;
        request.GroupId = groupId;
        request.MemberId = memberId;

        return WriteAndRead<TLeaveGroupResponseData>(header, request);
    }

    TMessagePtr<TOffsetFetchResponseData> OffsetFetch(TString groupId, std::map<TString, std::vector<i32>> topicsToPartions) {
        Cerr << ">>>>> TOffsetFetchRequestData\n";
        
        TRequestHeaderData header = Header(NKafka::EApiKey::OFFSET_FETCH, 8);

        TOffsetFetchRequestData::TOffsetFetchRequestGroup group;
        group.GroupId = groupId;

        for (const auto& [topicName, partitions] : topicsToPartions) {
            TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics topic;
            topic.Name = topicName;
            topic.PartitionIndexes = partitions;
            group.Topics.push_back(topic);
        }

        TOffsetFetchRequestData request;
        request.Groups.push_back(group);

        return WriteAndRead<TOffsetFetchResponseData>(header, request);
    }

    TMessagePtr<TOffsetFetchResponseData> OffsetFetch(TOffsetFetchRequestData request) {
        Cerr << ">>>>> TOffsetFetchRequestData\n";
        TRequestHeaderData header = Header(NKafka::EApiKey::OFFSET_FETCH, 8);
        return WriteAndRead<TOffsetFetchResponseData>(header, request);
    }

    TMessagePtr<TFetchResponseData> Fetch(const std::vector<std::pair<TString, std::vector<i32>>>& topics, i64 offset = 0) {
        Cerr << ">>>>> TFetchRequestData\n";

        TRequestHeaderData header = Header(NKafka::EApiKey::FETCH, 3);

        TFetchRequestData request;
        request.MaxBytes = 1024;
        request.MinBytes = 1;

        for (auto& topic: topics) {
            NKafka::TFetchRequestData::TFetchTopic topicReq {};
            topicReq.Topic = topic.first;
            for (auto& partition: topic.second) {
                NKafka::TFetchRequestData::TFetchTopic::TFetchPartition partitionReq {};
                partitionReq.FetchOffset = offset;
                partitionReq.Partition = partition;
                partitionReq.PartitionMaxBytes = 1024;
                topicReq.Partitions.push_back(partitionReq);
            }
            request.Topics.push_back(topicReq);
        }

        return WriteAndRead<TFetchResponseData>(header, request);
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

    template <std::derived_from<TApiMessage> T>
    TMessagePtr<T> WriteAndRead(TRequestHeaderData& header, TApiMessage& request) {
        Write(So, &header, &request);
        return Read<T>(Si, &header);
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
        TInsecureTestServer testServer("2");

        TString topicName = "/Root/topic-0-test";
        ui64 minActivePartitions = 10;

        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        {
            auto result =
                pqClient
                    .CreateTopic(topicName,
                                 NYdb::NTopic::TCreateTopicSettings()
                                    .PartitioningSettings(minActivePartitions, 100)
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
            UNIT_ASSERT_VALUES_EQUAL(msg->ApiKeys.size(), 15u);
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

            {
                std::vector<std::pair<TString, std::vector<i32>>> topics {{topicName, {0}}};
                auto msg = client.Fetch(topics);

                UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
                auto record = msg->Responses[0].Partitions[0].Records->Records[0];
              
                auto data = record.Value.value();
                auto dataStr = TString(data.data(), data.size());
                UNIT_ASSERT_VALUES_EQUAL(dataStr, value);
                
                auto headerKey = record.Headers[0].Key.value();
                auto headerKeyStr = TString(headerKey.data(), headerKey.size());
                UNIT_ASSERT_VALUES_EQUAL(dataStr, value);

                auto headerValue = record.Headers[0].Value.value();
                auto headerValueStr = TString(headerValue.data(), headerValue.size());
                UNIT_ASSERT_VALUES_EQUAL(dataStr, value);

            }

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

    Y_UNIT_TEST(FetchScenario) {
        TInsecureTestServer testServer("2");

        TString topicName = "/Root/topic-0-test";
        TString shortTopicName = "topic-0-test";
        TString notExistsTopicName = "/Root/not-exists";
        ui64 minActivePartitions = 10;

        TString key = "record-key";
        TString value = "record-value";
        TString headerKey = "header-key";
        TString headerValue = "header-value";

        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        {
            auto result =
                pqClient
                    .CreateTopic(topicName,
                                 NYdb::NTopic::TCreateTopicSettings()
                                    .PartitioningSettings(minActivePartitions, 100))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        TTestClient client(testServer.Port);

        {
            auto msg = client.ApiVersions();

            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(msg->ApiKeys.size(), 15u);
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
            // Check list offsets for empty topic
            std::vector<std::pair<i32,i64>> partitions {{0, FirstTopicOffset}, {0, LastTopicOffset}};
            auto msg = client.ListOffsets(partitions, topicName);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Partitions.size(), 2);
            
            for (auto& topic: msg->Topics) {
                for (auto& partition: topic.Partitions) {
                    UNIT_ASSERT_VALUES_EQUAL(partition.ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
                    UNIT_ASSERT_VALUES_EQUAL(partition.Offset, 0);
                }
            }
        }

        {
            // Check empty topic (no records)
            std::vector<std::pair<TString, std::vector<i32>>> topics {{topicName, {0}}};
            auto msg = client.Fetch(topics);

            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions[0].Records.has_value(), false);
        }

        {
            auto msg = client.InitProducerId();
            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }

        {
            // Produce
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
        }

        {
            // Check list offsets after produce
            std::vector<std::pair<i32,i64>> partitions {{0, FirstTopicOffset}, {0, LastTopicOffset}};
            auto msg = client.ListOffsets(partitions, topicName);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Partitions.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Partitions[0].Offset, 0);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Partitions[1].Offset, 1);
        }

        {
            // Check list offsets short topic name
            std::vector<std::pair<i32,i64>> partitions {{0, FirstTopicOffset}, {0, LastTopicOffset}};
            auto msg = client.ListOffsets(partitions, shortTopicName);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Partitions.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Partitions[0].Offset, 0);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Partitions[1].Offset, 1);
        }

        {
            // Check FETCH
            std::vector<std::pair<TString, std::vector<i32>>> topics {{topicName, {0}}};
            auto msg = client.Fetch(topics);
            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions[0].Records.has_value(), true);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions[0].Records->Records.size(), 1);
            auto record = msg->Responses[0].Partitions[0].Records->Records[0];

            auto data = record.Value.value();
            auto dataStr = TString(data.data(), data.size());
            UNIT_ASSERT_VALUES_EQUAL(dataStr, value);

            auto headerKey = record.Headers[0].Key.value();
            auto headerKeyStr = TString(headerKey.data(), headerKey.size());
            UNIT_ASSERT_VALUES_EQUAL(dataStr, value);

            auto headerValue = record.Headers[0].Value.value();
            auto headerValueStr = TString(headerValue.data(), headerValue.size());
            UNIT_ASSERT_VALUES_EQUAL(dataStr, value);
        }

        {
            // Check big offset
            std::vector<std::pair<TString, std::vector<i32>>> topics {{topicName, {0}}};
            auto msg = client.Fetch(topics, std::numeric_limits<i64>::max());
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions[0].ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::OFFSET_OUT_OF_RANGE));
        }

        {
            // Check short topic name
            std::vector<std::pair<TString, std::vector<i32>>> topics {{shortTopicName, {0}}};
            auto msg = client.Fetch(topics);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions[0].ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }

        {
            // Check not exists topics and partition
            std::vector<std::pair<TString, std::vector<i32>>> topics {
                {notExistsTopicName, {0}}, 
                {"", {0}},
                {topicName, {5000}},
                {topicName, {-1}}
                };
            auto msg = client.Fetch(topics);
            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), topics.size());
            for (size_t i = 0; i < topics.size(); i++) {
                UNIT_ASSERT_VALUES_EQUAL(msg->Responses[i].Partitions.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions[0].ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION));
            }         
        }

        //broken
        // {
        //     // Check partition double
        //     std::vector<std::pair<TString, std::vector<i32>>> topics {{topicName, {0,0}}};
        //     auto msg = client.Fetch(topics);
        //     UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        //     UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 1);
        //     UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions.size(), 2);
            
        //     for (size_t i = 0; i < 2; i++) {
        //         auto record = msg->Responses[0].Partitions[i].Records->Records[0];

        //         auto data = record.Value.value();
        //         auto dataStr = TString(data.data(), data.size());
        //         UNIT_ASSERT_VALUES_EQUAL(dataStr, value);
        //     }
        // }

        {
            // Check topic double
            std::vector<std::pair<TString, std::vector<i32>>> topics {{topicName, {0}},{topicName, {0}}};
            auto msg = client.Fetch(topics);
            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 2);
            
            for (size_t i = 0; i < 2; i++) {
                UNIT_ASSERT_VALUES_EQUAL(msg->Responses[i].Partitions.size(), 1);
                auto record = msg->Responses[i].Partitions[0].Records->Records[0];

                auto data = record.Value.value();
                auto dataStr = TString(data.data(), data.size());
                UNIT_ASSERT_VALUES_EQUAL(dataStr, value);
            }
        }

    } // Y_UNIT_TEST(FetchScenario)

    Y_UNIT_TEST(BalanceScenario) {
        TInsecureTestServer testServer("2");

        TString topicName = "/Root/topic-0-test";
        TString shortTopicName = "topic-0-test";

        TString secondTopicName = "/Root/topic-1-test";

        TString notExistsTopicName = "/Root/not-exists";

        ui64 minActivePartitions = 10;

        TString group = "consumer-0";
        TString notExistsGroup = "consumer-not-exists";

        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        {
            auto result =
                pqClient
                    .CreateTopic(topicName,
                                 NYdb::NTopic::TCreateTopicSettings()
                                    .PartitioningSettings(minActivePartitions, 100)
                                    .BeginAddConsumer(group).EndAddConsumer())
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        }

        {
            auto result =
                pqClient
                    .CreateTopic(secondTopicName,
                                 NYdb::NTopic::TCreateTopicSettings()
                                    .PartitioningSettings(minActivePartitions, 100)
                                    .BeginAddConsumer(group).EndAddConsumer())
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        }

        TTestClient clientA(testServer.Port);
        TTestClient clientB(testServer.Port);

        {
            auto msg = clientA.ApiVersions();

            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(msg->ApiKeys.size(), 15u);
        }

        {
            auto msg = clientA.SaslHandshake();

            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(msg->Mechanisms.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(*msg->Mechanisms[0], "PLAIN");
        }

        {
            auto msg = clientA.SaslAuthenticate("ouruser@/Root", "ourUserPassword");
            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }

        {
            // Check partitions balance
            std::vector<TString> topics;
            topics.push_back(topicName);

            // clientA join group, and get all partitions
            auto readInfoA = clientA.JoinAndSyncGroupAndWaitPartitions(topics, group, minActivePartitions);
            UNIT_ASSERT_VALUES_EQUAL(clientA.Heartbeat(readInfoA.MemberId, readInfoA.GenerationId, group)->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

            // clientB join group, and get 0 partitions, becouse it's all at clientA
            UNIT_ASSERT_VALUES_EQUAL(clientB.SaslHandshake()->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(clientB.SaslAuthenticate("ouruser@/Root", "ourUserPassword")->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            auto readInfoB = clientB.JoinAndSyncGroup(topics, group);
            UNIT_ASSERT_VALUES_EQUAL(readInfoB.Partitions.size(), 0);

            // clientA gets RABALANCE status, because of new reader. We need to release some partitions
            clientA.WaitRebalance(readInfoA.MemberId, readInfoA.GenerationId, group);

            // clientA now gets half of partitions
            readInfoA = clientA.JoinAndSyncGroupAndWaitPartitions(topics, group, minActivePartitions/2);
            UNIT_ASSERT_VALUES_EQUAL(clientA.Heartbeat(readInfoA.MemberId, readInfoA.GenerationId, group)->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

            // some partitions now released, and we can give them to clientB
            clientB.WaitRebalance(readInfoB.MemberId, readInfoB.GenerationId, group);
            readInfoB = clientB.JoinAndSyncGroupAndWaitPartitions(topics, group, minActivePartitions/2);
            UNIT_ASSERT_VALUES_EQUAL(clientB.Heartbeat(readInfoB.MemberId, readInfoB.GenerationId, group)->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

            // cleintA leave group and all partitions goes to clientB
            UNIT_ASSERT_VALUES_EQUAL(clientA.LeaveGroup(readInfoA.MemberId, group)->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            clientB.WaitRebalance(readInfoB.MemberId, readInfoB.GenerationId, group);
            readInfoB = clientB.JoinAndSyncGroupAndWaitPartitions(topics, group, minActivePartitions);
            UNIT_ASSERT_VALUES_EQUAL(clientB.Heartbeat(readInfoB.MemberId, readInfoB.GenerationId, group)->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

            // clientB leave group
            UNIT_ASSERT_VALUES_EQUAL(clientB.LeaveGroup(readInfoA.MemberId, group)->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));              
        }

        {
            // Check short topic name
            std::vector<TString> topics;
            topics.push_back(shortTopicName);

            auto joinResponse = clientA.JoinGroup(topics, group);
            UNIT_ASSERT_VALUES_EQUAL(joinResponse->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(clientA.LeaveGroup(joinResponse->MemberId.value(), group)->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }

        {
            // Check not exists group/consumer
            std::vector<TString> topics;
            topics.push_back(topicName);

            auto joinResponse = clientA.JoinGroup(topics, notExistsGroup);
            UNIT_ASSERT_VALUES_EQUAL(joinResponse->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::INVALID_REQUEST)); // because TReadInitAndAuthActor returns BAD_REQUEST on failed readRule check
        }

        {
            // Check not exists topic
            std::vector<TString> topics;
            topics.push_back(notExistsTopicName);
            
            auto joinResponse = clientA.JoinGroup(topics, group);
            UNIT_ASSERT_VALUES_EQUAL(joinResponse->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION));
        }

        {
            // Check few topics
            std::vector<TString> topics;
            topics.push_back(topicName);
            topics.push_back(secondTopicName);
            
            auto readInfo = clientA.JoinAndSyncGroupAndWaitPartitions(topics, group, minActivePartitions * 2);

            std::unordered_set<TString> topicsSet;
            for (auto partition: readInfo.Partitions) {
                topicsSet.emplace(partition.Topic.value());
            }
            UNIT_ASSERT_VALUES_EQUAL(topicsSet.size(), 2);


            // Check change topics list
            topics.pop_back();
            auto joinResponse = clientA.JoinGroup(topics, group);
            UNIT_ASSERT_VALUES_EQUAL(joinResponse->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::REBALANCE_IN_PROGRESS)); // tell client to rejoin
        }

    } // Y_UNIT_TEST(BalanceScenario)
    
    Y_UNIT_TEST(OffsetCommitAndFetchScenario) {
        TInsecureTestServer testServer("2");

        TString firstTopicName = "/Root/topic-0-test";
        TString secondTopicName = "/Root/topic-1-test";
        TString shortTopicName = "topic-1-test";
        TString notExistsTopicName = "/Root/not-exists";
        ui64 minActivePartitions = 10;

        TString firstConsumerName = "consumer-0";
        TString secondConsumerName = "consumer-1";
        TString notExistsConsumerName = "notExists";

        TString key = "record-key";
        TString value = "record-value";
        TString headerKey = "header-key";
        TString headerValue = "header-value";

        NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
        {
            auto result =
                pqClient
                    .CreateTopic(firstTopicName,
                                 NYdb::NTopic::TCreateTopicSettings()
                                    .BeginAddConsumer(firstConsumerName).EndAddConsumer()
                                    .BeginAddConsumer(secondConsumerName).EndAddConsumer()
                                    .PartitioningSettings(minActivePartitions, 100))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto result =
                pqClient
                    .CreateTopic(secondTopicName,
                                 NYdb::NTopic::TCreateTopicSettings()
                                    .BeginAddConsumer(firstConsumerName).EndAddConsumer()
                                    .BeginAddConsumer(secondConsumerName).EndAddConsumer()
                                    .PartitioningSettings(minActivePartitions, 100))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        TTestClient client(testServer.Port);

        {
            auto msg = client.ApiVersions();

            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(msg->ApiKeys.size(), 15u);
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

        auto recordsCount = 5;
        {
            // Produce

            TKafkaRecordBatch batch;
            batch.BaseOffset = 3;
            batch.BaseSequence = 5;
            batch.Magic = 2; // Current supported
            batch.Records.resize(recordsCount);

            for (auto i = 0; i < recordsCount; i++) {
                batch.Records[i].Key = TKafkaRawBytes(key.Data(), key.Size());
                batch.Records[i].Value = TKafkaRawBytes(value.Data(), value.Size());
            }      

            auto msg = client.Produce(firstTopicName, 0, batch);

            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Name, firstTopicName);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].Index, 0);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses[0].ErrorCode,
                                     static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }

        {
            // Fetch offsets
            std::map<TString, std::vector<i32>> topicsToPartions;
            topicsToPartions[firstTopicName] = std::vector<i32>{0, 1, 2, 3 };
            auto msg = client.OffsetFetch(firstConsumerName, topicsToPartions);
            UNIT_ASSERT_VALUES_EQUAL(msg->Groups.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics.size(), 1);
            const auto& partitions = msg->Groups[0].Topics[0].Partitions;
            UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 4);
            auto partition0 = std::find_if(partitions.begin(), partitions.end(), [](const auto& partition) { return partition.PartitionIndex == 0; });
            UNIT_ASSERT_VALUES_UNEQUAL(partition0, partitions.end());
            UNIT_ASSERT_VALUES_EQUAL(partition0->CommittedOffset, 0);
        }
        
        {
            // Check commit
            std::unordered_map<TString, std::vector<std::pair<ui64,ui64>>> offsets;
            std::vector<std::pair<ui64, ui64>> partitionsAndOffsets;
            for (ui64 i = 0; i < minActivePartitions; ++i) {
                partitionsAndOffsets.emplace_back(std::make_pair(i, recordsCount));
            }
            offsets[firstTopicName] = partitionsAndOffsets;
            offsets[shortTopicName] = partitionsAndOffsets;
            auto msg = client.OffsetCommit(firstConsumerName, offsets);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 2);
            for (const auto& topic : msg->Topics) {
                UNIT_ASSERT_VALUES_EQUAL(topic.Partitions.size(), minActivePartitions);
                for (const auto& partition : topic.Partitions) {
                    if (topic.Name.value() == firstTopicName) {
                        if (partition.PartitionIndex == 0) {
                            UNIT_ASSERT_VALUES_EQUAL(partition.ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
                        } else {
                            UNIT_ASSERT_VALUES_EQUAL(partition.ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::OFFSET_OUT_OF_RANGE));
                        }
                    } else {
                        UNIT_ASSERT_VALUES_EQUAL(partition.ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::OFFSET_OUT_OF_RANGE));
                    } 
                }
            }
        }

        {
            // Fetch offsets after commit
            std::map<TString, std::vector<i32>> topicsToPartions;
            topicsToPartions[firstTopicName] = std::vector<i32>{0, 1, 2 , 3 };
            auto msg = client.OffsetFetch(firstConsumerName, topicsToPartions);
            UNIT_ASSERT_VALUES_EQUAL(msg->Groups.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics.size(), 1);
            const auto& partitions = msg->Groups[0].Topics[0].Partitions;
            UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 4);
            auto partition0 = std::find_if(partitions.begin(), partitions.end(), [](const auto& partition) { return partition.PartitionIndex == 0; });
            UNIT_ASSERT_VALUES_UNEQUAL(partition0, partitions.end());
            UNIT_ASSERT_VALUES_EQUAL(partition0->CommittedOffset, 5);
        }

        {
            // Check fetch offsets with nonexistent topic
            std::map<TString, std::vector<i32>> topicsToPartions;
            topicsToPartions[notExistsTopicName] = std::vector<i32>{0, 1};
            auto msg = client.OffsetFetch(firstConsumerName, topicsToPartions);
            UNIT_ASSERT_VALUES_EQUAL(msg->Groups.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions.size(), 2);
            for (const auto& partition : msg->Groups[0].Topics[0].Partitions) {
                UNIT_ASSERT_VALUES_EQUAL(partition.ErrorCode, UNKNOWN_TOPIC_OR_PARTITION);
            }
        }

        {
            // Check commit with nonexistent topic
            std::unordered_map<TString, std::vector<std::pair<ui64,ui64>>> offsets;
            std::vector<std::pair<ui64, ui64>> partitionsAndOffsets;
            for (ui64 i = 0; i < minActivePartitions; ++i) {
                partitionsAndOffsets.emplace_back(std::make_pair(i, recordsCount));
            }
            offsets[firstTopicName] = partitionsAndOffsets;
            offsets[notExistsTopicName] = partitionsAndOffsets;

            auto msg = client.OffsetCommit(notExistsConsumerName, offsets);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.back().Partitions.size(), minActivePartitions);
            for (const auto& topic : msg->Topics) {
                for (const auto& partition : topic.Partitions) {
                   UNIT_ASSERT_VALUES_EQUAL(partition.ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::INVALID_REQUEST));
                }
            }
        }

        {
            // Check fetch offsets nonexistent consumer 
            std::map<TString, std::vector<i32>> topicsToPartions;
            topicsToPartions[firstTopicName] = std::vector<i32>{0, 1};
            auto msg = client.OffsetFetch(notExistsConsumerName, topicsToPartions);
            UNIT_ASSERT_VALUES_EQUAL(msg->Groups.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions.size(), 2);
            for (const auto& partition : msg->Groups[0].Topics[0].Partitions) {
                UNIT_ASSERT_VALUES_EQUAL(partition.ErrorCode, RESOURCE_NOT_FOUND);
            }
        }

        {
            // Check commit with nonexistent consumer
            std::unordered_map<TString, std::vector<std::pair<ui64,ui64>>> offsets;
            std::vector<std::pair<ui64, ui64>> partitionsAndOffsets;
            for (ui64 i = 0; i < minActivePartitions; ++i) {
                partitionsAndOffsets.emplace_back(std::make_pair(i, recordsCount));
            }
            offsets[firstTopicName] = partitionsAndOffsets;

            auto msg = client.OffsetCommit(notExistsConsumerName, offsets);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Topics.back().Partitions.size(), minActivePartitions);
            for (const auto& topic : msg->Topics) {
                for (const auto& partition : topic.Partitions) {
                   UNIT_ASSERT_VALUES_EQUAL(partition.ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::INVALID_REQUEST));
                }
            }
        }

        {
            // Check fetch offsets with 2 consumers and topics
            TOffsetFetchRequestData request;

            TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics topic;
            topic.Name = firstTopicName;
            auto partitionIndexes = std::vector<int>{0};
            topic.PartitionIndexes = partitionIndexes;

            TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics shortTopic;
            shortTopic.Name = shortTopicName;
            shortTopic.PartitionIndexes = partitionIndexes;

            TOffsetFetchRequestData::TOffsetFetchRequestGroup group0;
            group0.GroupId = firstConsumerName;
            group0.Topics.push_back(topic);
            request.Groups.push_back(group0);

            TOffsetFetchRequestData::TOffsetFetchRequestGroup group1;
            group1.GroupId = secondConsumerName;
            group1.Topics.push_back(shortTopic);
            request.Groups.push_back(group1);

            auto msg = client.OffsetFetch(request);

            UNIT_ASSERT_VALUES_EQUAL(msg->Groups.size(), 2);
            for (const auto& group: msg->Groups) {
                UNIT_ASSERT_VALUES_EQUAL(group.Topics.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(group.Topics[0].Partitions.size(), 1);
                if (group.GroupId == firstConsumerName) {
                    UNIT_ASSERT_VALUES_EQUAL(group.Topics[0].Partitions[0].CommittedOffset, 5);
                } else if (group.GroupId == secondConsumerName) {
                    UNIT_ASSERT_VALUES_EQUAL(group.Topics[0].Partitions[0].CommittedOffset, 0);
                }
                UNIT_ASSERT_VALUES_EQUAL(group.Topics[0].Partitions[0].ErrorCode, NONE_ERROR);
            }
        }
    } // Y_UNIT_TEST(OffsetFetchScenario)

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
            UNIT_ASSERT_VALUES_EQUAL(msg->ApiKeys.size(), 15u);
        }

        {
            auto msg = client.SaslHandshake();

            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT_VALUES_EQUAL(msg->Mechanisms.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(*msg->Mechanisms[0], "PLAIN");
        }

        {
            auto msg = client.SaslAuthenticate("@/Root", "ApiKey-value-valid");
            Cerr << msg->ErrorMessage << "\n";
            UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }

        Sleep(TDuration::Seconds(1));
    }
} // Y_UNIT_TEST_SUITE(KafkaProtocol)