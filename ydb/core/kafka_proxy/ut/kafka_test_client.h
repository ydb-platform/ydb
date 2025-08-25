#pragma once

#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/core/kafka_proxy/kafka_messages.h>
#include <ydb/core/kafka_proxy/kafka_topic_partition.h>
#include <ydb/core/kafka_proxy/actors/actors.h>

#include <util/system/tempfile.h>

using namespace NKafka;

static constexpr ui32 EXPECTED_API_KEYS_COUNT = 25u;
struct TTopicConfig {
    inline static const std::map<TString, TString> DummyMap;

    TTopicConfig(
            TString name,
            ui32 partionsNumber,
            std::optional<TString> retentionMs = std::nullopt,
            std::optional<TString> retentionBytes = std::nullopt,
            const std::map<TString, TString>& configs = DummyMap,
            TKafkaInt16 replicationFactor = 1)
        : Name(name)
        , PartitionsNumber(partionsNumber)
        , RetentionMs(retentionMs)
        , RetentionBytes(retentionBytes)
        , Configs(configs)
        , ReplicationFactor(replicationFactor)
    {
    }

    TString Name;
    ui32 PartitionsNumber;
    std::optional<TString> RetentionMs;
    std::optional<TString> RetentionBytes;
    std::map<TString, TString> Configs;
    TKafkaInt16 ReplicationFactor;
};

struct TReadInfo {
    std::vector<TConsumerProtocolAssignment::TopicPartition> Partitions;
    TString MemberId;
    i32 GenerationId;
};

class TKafkaTestClient {
    public:
        TKafkaTestClient(ui16 port, const TString clientName = "TestClient");

        template <std::derived_from<TApiMessage> T>
        void WriteToSocket(TRequestHeaderData& header, T& request) {
            Write(So, &header, &request);
        }

        template <std::derived_from<TApiMessage> T>
        TMessagePtr<T> ReadResponse(TRequestHeaderData& header) {
            return Read<T>(Si, &header);
        }

        TMessagePtr<TApiVersionsResponseData> ApiVersions(bool silent = false);

        TMessagePtr<TMetadataResponseData> Metadata(const TVector<TString>& topics = {}, std::optional<bool> allowAutoTopicCreation = std::nullopt);

        TMessagePtr<TSaslHandshakeResponseData> SaslHandshake(const TString& mechanism = "PLAIN");

        TMessagePtr<TSaslAuthenticateResponseData> SaslAuthenticate(const TString& user, const TString& password);

        TMessagePtr<TInitProducerIdResponseData> InitProducerId(const std::optional<TString>& transactionalId = {}, ui64 txnTimeoutMs = 1000);

        TMessagePtr<TOffsetCommitResponseData> OffsetCommit(TString groupId, std::unordered_map<TString, std::vector<NKafka::TEvKafka::PartitionConsumerOffset>> topicToConsumerOffsets);

        TMessagePtr<TProduceResponseData> Produce(const TString& topicName, ui32 partition, const TKafkaRecordBatch& batch);

        TMessagePtr<TProduceResponseData> Produce(const TString& topicName, const std::vector<std::pair<ui32, TKafkaRecordBatch>>& msgs, const std::optional<TString>& transactionalId = {});

        TMessagePtr<TProduceResponseData> Produce(const TTopicPartition& topicPartition,
                                                  const std::vector<std::pair<TString, TString>>& keyValueMessages,
                                                  ui32 baseSequence = 0,
                                                  const std::optional<TProducerInstanceId>& producerInstanceId = {},
                                                  const std::optional<TString>& transactionalId = {});

        TMessagePtr<TListOffsetsResponseData> ListOffsets(std::vector<std::pair<i32,i64>>& partitions, const TString& topic);

        TMessagePtr<TJoinGroupResponseData> JoinGroup(std::vector<TString>& topics, TString& groupId, TString protocolName, i32 heartbeatTimeout = 1000000);

        TMessagePtr<TSyncGroupResponseData> SyncGroup(TString& memberId, ui64 generationId, TString& groupId, std::vector<NKafka::TSyncGroupRequestData::TSyncGroupRequestAssignment> assignments, TString& protocolName);

        TReadInfo JoinAndSyncGroup(std::vector<TString>& topics, TString& groupId, TString& protocolName, i32 heartbeatTimeout = 1000000, ui32 totalPartitionsCount = 0);

        TMessagePtr<THeartbeatResponseData> Heartbeat(TString& memberId, ui64 generationId, TString& groupId);

        void WaitRebalance(TString& memberId, ui64 generationId, TString& groupId);

        TReadInfo JoinAndSyncGroupAndWaitPartitions(std::vector<TString>& topics, TString& groupId, ui32 expectedPartitionsCount, TString& protocolName, ui32 totalPartitionsCount = 0, ui32 hartbeatTimeout = 1000000);

        TMessagePtr<TLeaveGroupResponseData> LeaveGroup(TString& memberId, TString& groupId);

        TConsumerProtocolAssignment GetAssignments(NKafka::TSyncGroupResponseData::AssignmentMeta::Type metadata);

        std::vector<NKafka::TSyncGroupRequestData::TSyncGroupRequestAssignment> MakeRangeAssignment(
            TMessagePtr<TJoinGroupResponseData>& joinResponse,
            int totalPartitionsCount);

        TMessagePtr<TOffsetFetchResponseData> OffsetFetch(TString groupId, std::map<TString, std::vector<i32>> topicsToPartions);

        TMessagePtr<TOffsetFetchResponseData> OffsetFetch(TOffsetFetchRequestData& request);

        TMessagePtr<TListGroupsResponseData> ListGroups(TListGroupsRequestData request);

        TMessagePtr<TListGroupsResponseData> ListGroups(const std::vector<std::optional<TString>>& statesFilter = {});

        TMessagePtr<TDescribeGroupsResponseData> DescribeGroups(TDescribeGroupsRequestData& request);

        TMessagePtr<TDescribeGroupsResponseData> DescribeGroups(const std::vector<std::optional<TString>>& groups);

        TMessagePtr<TFetchResponseData> Fetch(const std::vector<std::pair<TKafkaUuid, std::vector<i32>>>& topics, i64 offset = 0);
        TMessagePtr<TFetchResponseData> Fetch(const std::vector<std::pair<TString, std::vector<i32>>>& topics, i64 offset = 0);
        void ValidateNoDataInTopics(const std::vector<std::pair<TString, std::vector<i32>>>& topics, i64 offset = 0);

        TMessagePtr<TCreateTopicsResponseData> CreateTopics(std::vector<TTopicConfig> topicsToCreate, bool validateOnly = false);

        TMessagePtr<TCreatePartitionsResponseData> CreatePartitions(const std::vector<TTopicConfig>& topicsToCreate, bool validateOnly = false);

        TMessagePtr<TAlterConfigsResponseData> AlterConfigs(std::vector<TTopicConfig> topicsToModify, bool validateOnly = false);

        TMessagePtr<TDescribeConfigsResponseData> DescribeConfigs(std::vector<TString> topics);

        TMessagePtr<TAddPartitionsToTxnResponseData> AddPartitionsToTxn(const TString& transactionalId, const TProducerInstanceId& producerInstanceId, const std::unordered_map<TString, std::vector<ui32>>& topicPartitions);

        TMessagePtr<TAddOffsetsToTxnResponseData> AddOffsetsToTxn(const TString& transactionalId, const TProducerInstanceId& producerInstanceId, const TString& groupId);

        TMessagePtr<TTxnOffsetCommitResponseData> TxnOffsetCommit(const TString& transactionalId, const TProducerInstanceId& producerInstanceId, const TString& groupName, ui32 generation, const std::unordered_map<TString, std::vector<std::pair<ui32, ui64>>>& paritionOffsetsToTopic);

        TMessagePtr<TEndTxnResponseData> EndTxn(const TString& transactionalId, const TProducerInstanceId& producerInstanceId, bool commit);

        void UnknownApiKey();

        void AuthenticateToKafka();

        void AuthenticateToKafka(const TString& userName, const TString& userPassword);

        TRequestHeaderData Header(NKafka::EApiKey apiKey, TKafkaVersion version);

    protected:
        ui32 NextCorrelation();
        template <std::derived_from<TApiMessage> T>
        TMessagePtr<T> WriteAndRead(TRequestHeaderData& header, TApiMessage& request, bool silent = false);
        void Write(TSocketOutput& so, TApiMessage* request, TKafkaVersion version, bool silent = false);
        void Write(TSocketOutput& so, TRequestHeaderData* header, TApiMessage* request, bool silent = false);
        template <std::derived_from<TApiMessage> T>
        TMessagePtr<T> Read(TSocketInput& si, TRequestHeaderData* requestHeader);
        void Print(const TBuffer& buffer);
        char Hex0(const unsigned char c);
        void FillTopicsFromJoinGroupMetadata(TKafkaBytes& metadata, THashSet<TString>& topics);

    private:
        TNetworkAddress Addr;
        TSocket Socket;
        TSocketOutput So;
        TSocketInput Si;

        ui32 Correlation;
        TString ClientName;
    };
