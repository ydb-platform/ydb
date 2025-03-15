#include "kafka_test_client.h"

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/kafka_proxy/kafka_constants.h>

static constexpr TKafkaUint16 ASSIGNMENT_VERSION = 3;

using namespace NKafka;

TKafkaTestClient::TKafkaTestClient(ui16 port, const TString clientName)
    : Addr("localhost", port)
    , Socket(Addr)
    , So(Socket)
    , Si(Socket)
    , Correlation(0)
    , ClientName(clientName) {
}

TMessagePtr<TApiVersionsResponseData> TKafkaTestClient::ApiVersions() {
    Cerr << ">>>>> ApiVersionsRequest\n";

    TRequestHeaderData header = Header(NKafka::EApiKey::API_VERSIONS, 2);

    TApiVersionsRequestData request;
    request.ClientSoftwareName = "SuperTest";
    request.ClientSoftwareVersion = "3100.7.13";

    return WriteAndRead<TApiVersionsResponseData>(header, request);
}

TMessagePtr<TMetadataResponseData> TKafkaTestClient::Metadata(const TVector<TString>& topics) {
    Cerr << ">>>>> MetadataRequest\n";

    TRequestHeaderData header = Header(NKafka::EApiKey::METADATA, 9);

    TMetadataRequestData request;
    request.Topics.reserve(topics.size());
    for (auto topicName : topics) {
        NKafka::TMetadataRequestData::TMetadataRequestTopic topic;
        topic.Name = topicName;
        request.Topics.push_back(topic);
    }

    return WriteAndRead<TMetadataResponseData>(header, request);
}

TMessagePtr<TSaslHandshakeResponseData> TKafkaTestClient::SaslHandshake(const TString& mechanism) {
    Cerr << ">>>>> SaslHandshakeRequest\n";

    TRequestHeaderData header = Header(NKafka::EApiKey::SASL_HANDSHAKE, 1);

    TSaslHandshakeRequestData request;
    request.Mechanism = mechanism;

    return WriteAndRead<TSaslHandshakeResponseData>(header, request);
}

TMessagePtr<TSaslAuthenticateResponseData> TKafkaTestClient::SaslAuthenticate(const TString& user, const TString& password) {
    Cerr << ">>>>> SaslAuthenticateRequestData\n";

    TStringBuilder authBytes;
    authBytes << "ignored" << '\0' << user << '\0' << password;

    TRequestHeaderData header = Header(NKafka::EApiKey::SASL_AUTHENTICATE, 2);

    TSaslAuthenticateRequestData request;
    request.AuthBytes = TKafkaRawBytes(authBytes.data(), authBytes.size());

    return WriteAndRead<TSaslAuthenticateResponseData>(header, request);
}

TMessagePtr<TInitProducerIdResponseData> TKafkaTestClient::InitProducerId() {
    Cerr << ">>>>> TInitProducerIdRequestData\n";

    TRequestHeaderData header = Header(NKafka::EApiKey::INIT_PRODUCER_ID, 4);

    TInitProducerIdRequestData request;
    request.TransactionTimeoutMs = 5000;

    return WriteAndRead<TInitProducerIdResponseData>(header, request);
}

TMessagePtr<TOffsetCommitResponseData> TKafkaTestClient::OffsetCommit(TString groupId, std::unordered_map<TString, std::vector<std::pair<ui64,ui64>>> topicsToPartions) {
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

TMessagePtr<TProduceResponseData> TKafkaTestClient::Produce(const TString& topicName, ui32 partition, const TKafkaRecordBatch& batch) {
    std::vector<std::pair<ui32, TKafkaRecordBatch>> msgs;
    msgs.emplace_back(partition, batch);
    return Produce(topicName, msgs);
}

TMessagePtr<TProduceResponseData> TKafkaTestClient::Produce(const TString& topicName, const std::vector<std::pair<ui32, TKafkaRecordBatch>> msgs) {
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

TMessagePtr<TListOffsetsResponseData> TKafkaTestClient::ListOffsets(std::vector<std::pair<i32,i64>>& partitions, const TString& topic) {
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

TMessagePtr<TJoinGroupResponseData> TKafkaTestClient::JoinGroup(std::vector<TString>& topics, TString& groupId, TString protocolName, i32 heartbeatTimeout) {
    Cerr << ">>>>> TJoinGroupRequestData\n";

    TRequestHeaderData header = Header(NKafka::EApiKey::JOIN_GROUP, 9);

    TJoinGroupRequestData request;
    request.GroupId = groupId;
    request.ProtocolType = "consumer";
    request.SessionTimeoutMs = heartbeatTimeout;

    NKafka::TJoinGroupRequestData::TJoinGroupRequestProtocol protocol;
    protocol.Name = protocolName;

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

TMessagePtr<TSyncGroupResponseData> TKafkaTestClient::SyncGroup(TString& memberId, ui64 generationId, TString& groupId, std::vector<NKafka::TSyncGroupRequestData::TSyncGroupRequestAssignment> assignments, TString& protocolName) {
    Cerr << ">>>>> TSyncGroupRequestData\n";

    TRequestHeaderData header = Header(NKafka::EApiKey::SYNC_GROUP, 5);

    TSyncGroupRequestData request;
    request.GroupId = groupId;
    request.ProtocolType = "consumer";
    request.ProtocolName = protocolName;
    request.GenerationId = generationId;
    request.GroupId = groupId;
    request.MemberId = memberId;

    request.Assignments = assignments;

    return WriteAndRead<TSyncGroupResponseData>(header, request);
}

TReadInfo TKafkaTestClient::JoinAndSyncGroup(std::vector<TString>& topics, TString& groupId, TString& protocolName, i32 heartbeatTimeout, ui32 totalPartitionsCount) {
    auto joinResponse = JoinGroup(topics, groupId, protocolName, heartbeatTimeout);
    auto memberId = joinResponse->MemberId;
    auto generationId = joinResponse->GenerationId;
    auto balanceStrategy = joinResponse->ProtocolName;
    UNIT_ASSERT_VALUES_EQUAL(joinResponse->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

    const bool isLeader = (joinResponse->Leader == memberId);
    std::vector<NKafka::TSyncGroupRequestData::TSyncGroupRequestAssignment> assignments;
    if (isLeader) {
        assignments = MakeRangeAssignment(joinResponse, totalPartitionsCount);
    }

    auto syncResponse = SyncGroup(memberId.value(), generationId, groupId, assignments, protocolName);
    UNIT_ASSERT_VALUES_EQUAL(syncResponse->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

    TReadInfo readInfo;
    readInfo.GenerationId = generationId;
    readInfo.MemberId = memberId.value();
    readInfo.Partitions = GetAssignments(syncResponse->Assignment).AssignedPartitions;
    return readInfo;
}

TMessagePtr<THeartbeatResponseData> TKafkaTestClient::Heartbeat(TString& memberId, ui64 generationId, TString& groupId) {
    Cerr << ">>>>> THeartbeatRequestData\n";

    TRequestHeaderData header = Header(NKafka::EApiKey::HEARTBEAT, 4);

    THeartbeatRequestData request;
    request.GroupId = groupId;
    request.MemberId = memberId;
    request.GenerationId = generationId;

    return WriteAndRead<THeartbeatResponseData>(header, request);
}

void TKafkaTestClient::WaitRebalance(TString& memberId, ui64 generationId, TString& groupId) {
    TKafkaInt16 heartbeatStatus;
    do {
        heartbeatStatus = Heartbeat(memberId, generationId, groupId)->ErrorCode;
    } while (heartbeatStatus == static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

    UNIT_ASSERT_VALUES_EQUAL(heartbeatStatus, static_cast<TKafkaInt16>(EKafkaErrors::REBALANCE_IN_PROGRESS));
}

TReadInfo TKafkaTestClient::JoinAndSyncGroupAndWaitPartitions(std::vector<TString>& topics, TString& groupId, ui32 expectedPartitionsCount, TString& protocolName, ui32 totalPartitionsCount, ui32 hartbeatTimeout) {
    TReadInfo readInfo;
    for (;;) {
        readInfo = JoinAndSyncGroup(topics, groupId, protocolName, hartbeatTimeout, totalPartitionsCount);
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

TMessagePtr<TLeaveGroupResponseData> TKafkaTestClient::LeaveGroup(TString& memberId, TString& groupId) {
    Cerr << ">>>>> TLeaveGroupRequestData\n";

    TRequestHeaderData header = Header(NKafka::EApiKey::LEAVE_GROUP, 2);

    TLeaveGroupRequestData request;
    request.GroupId = groupId;
    request.MemberId = memberId;

    return WriteAndRead<TLeaveGroupResponseData>(header, request);
}

TConsumerProtocolAssignment TKafkaTestClient::GetAssignments(NKafka::TSyncGroupResponseData::AssignmentMeta::Type metadata) {
    TKafkaVersion version = *(TKafkaVersion*)(metadata.value().data() + sizeof(TKafkaVersion));
    TBuffer buffer(metadata.value().data() + sizeof(TKafkaVersion), metadata.value().size_bytes() - sizeof(TKafkaVersion));
    TKafkaReadable readable(buffer);

    TConsumerProtocolAssignment result;
    result.Read(readable, version);

    return result;
}

std::vector<NKafka::TSyncGroupRequestData::TSyncGroupRequestAssignment> TKafkaTestClient::MakeRangeAssignment(
    TMessagePtr<TJoinGroupResponseData>& joinResponse,
    int totalPartitionsCount)
{

    std::vector<NKafka::TSyncGroupRequestData::TSyncGroupRequestAssignment> assignments;

    std::unordered_map<TString, THashSet<TString>> memberToTopics;

    for (auto& member : joinResponse->Members) {
        THashSet<TString> memberTopics;
        FillTopicsFromJoinGroupMetadata(member.Metadata, memberTopics);
        memberToTopics[member.MemberId.value()] = std::move(memberTopics);
    }

    THashSet<TString> allTopics;
    for (auto& kv : memberToTopics) {
        for (auto& t : kv.second) {
            allTopics.insert(t);
        }
    }

    std::unordered_map<TString, std::vector<TString>> topicToMembers;
    for (auto& t : allTopics) {
        for (auto& [mId, topicsSet] : memberToTopics) {
            if (topicsSet.contains(t)) {
                topicToMembers[t].push_back(mId);
            }
        }
    }

    for (const auto& member : joinResponse->Members) {
        TConsumerProtocolAssignment consumerAssignment;

        const auto& requestedTopics = memberToTopics[member.MemberId.value()];
        for (auto& topicName : requestedTopics) {

            auto& interestedMembers = topicToMembers[topicName];
            auto it = std::find(interestedMembers.begin(), interestedMembers.end(), member.MemberId);
            if (it == interestedMembers.end()) {
                continue;
            }

            int idx = static_cast<int>(std::distance(interestedMembers.begin(), it));
            int totalInterested = static_cast<int>(interestedMembers.size());

            const int totalPartitions = totalPartitionsCount;

            int baseCount = totalPartitions / totalInterested;
            int remainder = totalPartitions % totalInterested;

            int start = idx * baseCount + std::min<int>(idx, remainder);
            int length = baseCount + (idx < remainder ? 1 : 0);


            TConsumerProtocolAssignment::TopicPartition topicPartition;
            topicPartition.Topic = topicName;
            for (int p = start; p < start + length; ++p) {
                topicPartition.Partitions.push_back(p);
            }
            consumerAssignment.AssignedPartitions.push_back(topicPartition);
        }

        {
            TWritableBuf buf(nullptr, consumerAssignment.Size(ASSIGNMENT_VERSION) + sizeof(ASSIGNMENT_VERSION));
            TKafkaWritable writable(buf);

            writable << ASSIGNMENT_VERSION;
            consumerAssignment.Write(writable, ASSIGNMENT_VERSION);
            NKafka::TSyncGroupRequestData::TSyncGroupRequestAssignment syncAssignment;
            syncAssignment.MemberId = member.MemberId;
            syncAssignment.AssignmentStr = TString(buf.GetBuffer().data(), buf.GetBuffer().size());
            syncAssignment.Assignment = syncAssignment.AssignmentStr;

            assignments.push_back(std::move(syncAssignment));
        }
    }

    return assignments;
}

TMessagePtr<TOffsetFetchResponseData> TKafkaTestClient::OffsetFetch(TString groupId, std::map<TString, std::vector<i32>> topicsToPartions) {
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

TMessagePtr<TOffsetFetchResponseData> TKafkaTestClient::OffsetFetch(TOffsetFetchRequestData request) {
    Cerr << ">>>>> TOffsetFetchRequestData\n";
    TRequestHeaderData header = Header(NKafka::EApiKey::OFFSET_FETCH, 8);
    return WriteAndRead<TOffsetFetchResponseData>(header, request);
}

TMessagePtr<TFetchResponseData> TKafkaTestClient::Fetch(const std::vector<std::pair<TString, std::vector<i32>>>& topics, i64 offset) {
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

TMessagePtr<TCreateTopicsResponseData> TKafkaTestClient::CreateTopics(std::vector<TTopicConfig> topicsToCreate, bool validateOnly) {
    Cerr << ">>>>> TCreateTopicsRequestData\n";

    TRequestHeaderData header = Header(NKafka::EApiKey::CREATE_TOPICS, 7);
    TCreateTopicsRequestData request;
    request.ValidateOnly = validateOnly;

    for (auto& topicToCreate : topicsToCreate) {
        NKafka::TCreateTopicsRequestData::TCreatableTopic topic;
        topic.Name = topicToCreate.Name;
        topic.NumPartitions = topicToCreate.PartitionsNumber;

        auto addConfig = [&topic](std::optional<TString> configValue, TString configName) {
            if (configValue.has_value()) {
                NKafka::TCreateTopicsRequestData::TCreatableTopic::TCreateableTopicConfig config;
                config.Name = configName;
                config.Value = configValue.value();
                topic.Configs.push_back(config);
            }
        };

        addConfig(topicToCreate.RetentionMs, "retention.ms");
        addConfig(topicToCreate.RetentionBytes, "retention.bytes");

        for (auto const& [name, value] : topicToCreate.Configs) {
            NKafka::TCreateTopicsRequestData::TCreatableTopic::TCreateableTopicConfig config;
            config.Name = name;
            config.Value = value;
            topic.Configs.push_back(config);
        }

        request.Topics.push_back(topic);
    }

    return WriteAndRead<TCreateTopicsResponseData>(header, request);
}

TMessagePtr<TCreatePartitionsResponseData> TKafkaTestClient::CreatePartitions(std::vector<TTopicConfig> topicsToCreate, bool validateOnly) {
    Cerr << ">>>>> TCreateTopicsRequestData\n";

    TRequestHeaderData header = Header(NKafka::EApiKey::CREATE_PARTITIONS, 3);
    TCreatePartitionsRequestData request;
    request.ValidateOnly = validateOnly;
    request.TimeoutMs = 100;

    for (auto& topicToCreate : topicsToCreate) {
        NKafka::TCreatePartitionsRequestData::TCreatePartitionsTopic topic;
        topic.Name = topicToCreate.Name;
        topic.Count = topicToCreate.PartitionsNumber;

        request.Topics.push_back(topic);
    }

    return WriteAndRead<TCreatePartitionsResponseData>(header, request);
}

TMessagePtr<TAlterConfigsResponseData> TKafkaTestClient::AlterConfigs(std::vector<TTopicConfig> topicsToModify, bool validateOnly) {
    Cerr << ">>>>> TAlterConfigsRequestData\n";

    TRequestHeaderData header = Header(NKafka::EApiKey::ALTER_CONFIGS, 2);
    TAlterConfigsRequestData request;
    request.ValidateOnly = validateOnly;

    for (auto& topicToModify : topicsToModify) {
        NKafka::TAlterConfigsRequestData::TAlterConfigsResource resource;
        resource.ResourceType = TOPIC_RESOURCE_TYPE;
        resource.ResourceName = topicToModify.Name;

        auto addConfig = [&resource](std::optional<TString> configValue, TString configName) {
            if (configValue.has_value()) {
                NKafka::TAlterConfigsRequestData::TAlterConfigsResource::TAlterableConfig config;
                config.Name = configName;
                config.Value = configValue.value();
                resource.Configs.push_back(config);
            }
        };

        addConfig(topicToModify.RetentionMs, "retention.ms");
        addConfig(topicToModify.RetentionBytes, "retention.bytes");

        for (auto const& [name, value] : topicToModify.Configs) {
            NKafka::TAlterConfigsRequestData::TAlterConfigsResource::TAlterableConfig config;
            config.Name = name;
            config.Value = value;
            resource.Configs.push_back(config);
        }
        request.Resources.push_back(resource);
    }

    return WriteAndRead<TAlterConfigsResponseData>(header, request);
}

void TKafkaTestClient::UnknownApiKey() {
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

void TKafkaTestClient::AuthenticateToKafka() {
{
        auto msg = ApiVersions();

        UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(msg->ApiKeys.size(), 18u);
    }

    {
        auto msg = SaslHandshake();

        UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(msg->Mechanisms.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(*msg->Mechanisms[0], "PLAIN");
    }

    {
        auto msg = SaslAuthenticate("ouruser@/Root", "ourUserPassword");
        UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
    }
}


TRequestHeaderData TKafkaTestClient::Header(NKafka::EApiKey apiKey, TKafkaVersion version) {
    TRequestHeaderData header;
    header.RequestApiKey = apiKey;
    header.RequestApiVersion = version;
    header.CorrelationId = NextCorrelation();
    header.ClientId = ClientName;
    return header;
}

ui32 TKafkaTestClient::NextCorrelation() {
    return Correlation++;
}

template <std::derived_from<TApiMessage> T>
TMessagePtr<T> TKafkaTestClient::WriteAndRead(TRequestHeaderData& header, TApiMessage& request) {
    Write(So, &header, &request);
    return Read<T>(Si, &header);
}

void TKafkaTestClient::Write(TSocketOutput& so, TApiMessage* request, TKafkaVersion version) {
    TWritableBuf sb(nullptr, request->Size(version) + 1000);
    TKafkaWritable writable(sb);
    request->Write(writable, version);
    so.Write(sb.Data(), sb.Size());

    Print(sb.GetBuffer());
}

void TKafkaTestClient::Write(TSocketOutput& so, TRequestHeaderData* header, TApiMessage* request) {
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
TMessagePtr<T> TKafkaTestClient::Read(TSocketInput& si, TRequestHeaderData* requestHeader) {
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

void TKafkaTestClient::Print(const TBuffer& buffer) {
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

char TKafkaTestClient::Hex0(const unsigned char c) {
    return c < 10 ? '0' + c : 'A' + c - 10;
}

void TKafkaTestClient::FillTopicsFromJoinGroupMetadata(TKafkaBytes& metadata, THashSet<TString>& topics) {
    TKafkaVersion version = *(TKafkaVersion*)(metadata.value().data() + sizeof(TKafkaVersion));

    TBuffer buffer(metadata.value().data() + sizeof(TKafkaVersion), metadata.value().size_bytes() - sizeof(TKafkaVersion));
    TKafkaReadable readable(buffer);

    TConsumerProtocolSubscription result;
    result.Read(readable, version);

    for (auto topic: result.Topics) {
        if (topic.has_value()) {
            topics.emplace(topic.value());
        }
    }
}