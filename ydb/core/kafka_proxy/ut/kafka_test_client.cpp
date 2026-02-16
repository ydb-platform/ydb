#include "kafka_test_client.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <ydb/core/kafka_proxy/kafka_constants.h>
#include <ydb/library/login/sasl/scram.h>

#include <util/random/random.h>

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

TMessagePtr<TApiVersionsResponseData> TKafkaTestClient::ApiVersions(bool silent) {
    if (!silent) {
        Cerr << ">>>>> ApiVersionsRequest\n";
    }

    TRequestHeaderData header = Header(NKafka::EApiKey::API_VERSIONS, 2);

    TApiVersionsRequestData request;
    request.ClientSoftwareName = "SuperTest";
    request.ClientSoftwareVersion = "3100.7.13";

    return WriteAndRead<TApiVersionsResponseData>(header, request, silent);
}

// YDB ignores AllowAutoTopicCreation, i.e. it never creates a new topic implicitly.
// But in Apache Kafka the default behavior is to create a new topic, if there is no one at the moment of the request.
// With this flag, allowAutoTopicCreation, you can stop this behavior in Apache Kafka.
TMessagePtr<TMetadataResponseData> TKafkaTestClient::Metadata(const TVector<TString>& topics, bool allowAutoTopicCreation) {
    Cerr << ">>>>> MetadataRequest\n";

    TRequestHeaderData header = Header(NKafka::EApiKey::METADATA, 12);

    TMetadataRequestData request;
    request.AllowAutoTopicCreation = allowAutoTopicCreation;
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

TMessagePtr<TSaslAuthenticateResponseData> TKafkaTestClient::SaslPlainAuthenticate(const TString& user, const TString& password) {
    Cerr << ">>>>> SaslAuthenticateRequestData\n";

    TStringBuilder authBytes;
    authBytes << "authzid" << '\0' << user << '\0' << password;

    TRequestHeaderData header = Header(NKafka::EApiKey::SASL_AUTHENTICATE, 2);

    TSaslAuthenticateRequestData request;
    request.AuthBytes = ToRawBytes(authBytes);

    return WriteAndRead<TSaslAuthenticateResponseData>(header, request);
}

TMessagePtr<TSaslAuthenticateResponseData> TKafkaTestClient::SaslScramAuthenticateFirstMsg(const TString& user, const TString& clientNonce) {
    Cerr << ">>>>> SaslAuthenticateRequestData (SCRAM first message)\n";

    // Build first client message
    TString gs2Header = "n,,"; // No channel binding, no authzid
    TString clientFirstMessageBare = TStringBuilder() << "n=" << user << ",r=" << clientNonce;
    TString clientFirstMessage = gs2Header + clientFirstMessageBare;

    TRequestHeaderData header = Header(NKafka::EApiKey::SASL_AUTHENTICATE, 2);
    TSaslAuthenticateRequestData request;
    request.AuthBytes = ToRawBytes(clientFirstMessage);

    return WriteAndRead<TSaslAuthenticateResponseData>(header, request);
}

TMessagePtr<TSaslAuthenticateResponseData> TKafkaTestClient::SaslScramAuthenticateFinalMsg(const TString& nonce, const TString& clientProof) {
    Cerr << ">>>>> SaslAuthenticateRequestData (SCRAM final message)\n";

    // Build client final message without proof
    TString gs2Header = "n,,"; // No channel binding, no authzid
    TString channelBinding = Base64Encode(gs2Header);
    TString clientFinalMessageWithoutProof = TStringBuilder() << "c=" << channelBinding << ",r=" << nonce;

    // Build final client message (clientProof is already base64 encoded)
    TString clientFinalMessage = clientFinalMessageWithoutProof + ",p=" + clientProof;

    TRequestHeaderData header = Header(NKafka::EApiKey::SASL_AUTHENTICATE, 2);
    TSaslAuthenticateRequestData request;
    request.AuthBytes = ToRawBytes(clientFinalMessage);

    return WriteAndRead<TSaslAuthenticateResponseData>(header, request);
}

TMessagePtr<TInitProducerIdResponseData> TKafkaTestClient::InitProducerId(const std::optional<TString>& transactionalId, ui64 txnTimeoutMs) {
    Cerr << ">>>>> TInitProducerIdRequestData\n";

    TRequestHeaderData header = Header(NKafka::EApiKey::INIT_PRODUCER_ID, 4);

    TInitProducerIdRequestData request;
    request.TransactionalId = transactionalId;
    request.TransactionTimeoutMs = txnTimeoutMs;

    return WriteAndRead<TInitProducerIdResponseData>(header, request);
}


TMessagePtr<TOffsetCommitResponseData> TKafkaTestClient::OffsetCommit(TString groupId, std::unordered_map<TString, std::vector<NKafka::TEvKafka::PartitionConsumerOffset>> topicToConsumerOffsets) {
    Cerr << ">>>>> TOffsetCommitRequestData\n";

    TRequestHeaderData header = Header(NKafka::EApiKey::OFFSET_COMMIT, 1);

    TOffsetCommitRequestData request;
    request.GroupId = groupId;

    for (const auto& topicToConsumerOffsets : topicToConsumerOffsets) {
        NKafka::TOffsetCommitRequestData::TOffsetCommitRequestTopic topic;
        topic.Name = topicToConsumerOffsets.first;

        for (auto partitionAndOffset : topicToConsumerOffsets.second) {
            NKafka::TOffsetCommitRequestData::TOffsetCommitRequestTopic::TOffsetCommitRequestPartition partition;
            partition.PartitionIndex = partitionAndOffset.PartitionIndex;
            partition.CommittedOffset = partitionAndOffset.Offset;
            partition.CommittedMetadata = partitionAndOffset.Metadata;
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

TMessagePtr<TProduceResponseData> TKafkaTestClient::Produce(const TString& topicName, const std::vector<std::pair<ui32, TKafkaRecordBatch>>& msgs, const std::optional<TString>& transactionalId) {
    Cerr << ">>>>> TProduceRequestData\n";

    TRequestHeaderData header = Header(NKafka::EApiKey::PRODUCE, 9);

    TProduceRequestData request;
    request.Acks = -1;
    request.TopicData.resize(1);
    request.TopicData[0].Name = topicName;
    request.TopicData[0].PartitionData.resize(msgs.size());
    for(size_t i = 0 ; i < msgs.size(); ++i) {
        request.TopicData[0].PartitionData[i].Index = msgs[i].first;
        request.TopicData[0].PartitionData[i].Records = msgs[i].second;
    }

    if (transactionalId) {
        request.TransactionalId = *transactionalId;
    }

    return WriteAndRead<TProduceResponseData>(header, request);
}

void TKafkaTestClient::ProduceAsync(const TTopicPartition& topicPartition,
                                                            const std::vector<std::pair<TString, TString>>& keyValueMessages,
                                                            ui32 baseSequence,
                                                            const std::optional<TProducerInstanceId>& producerInstanceId,
                                                            const std::optional<TString>& transactionalId) {
    Cerr << ">>>>> TProduceRequestData\n";

    TKafkaRecordBatch batch;
    batch.BaseSequence = baseSequence;
    batch.Magic = TKafkaRecordBatch::MagicMeta::Default;
    batch.Records.resize(keyValueMessages.size());
    for (ui32 i = 0; i < keyValueMessages.size(); i++) {
        auto& keyValueMessage = keyValueMessages[i];
        batch.Records[i].Key = ToRawBytes(keyValueMessage.first);
        batch.Records[i].Value = ToRawBytes(keyValueMessage.second);
    }
    if (producerInstanceId) {
        batch.ProducerId = producerInstanceId->Id;
        batch.ProducerEpoch = producerInstanceId->Epoch;
    }
    std::vector<std::pair<ui32, TKafkaRecordBatch>> msgs;
    msgs.emplace_back(topicPartition.PartitionId, std::move(batch));

    TString topicName = topicPartition.TopicPath;

    TRequestHeaderData header = Header(NKafka::EApiKey::PRODUCE, 9);

    TProduceRequestData request;
    request.Acks = -1;
    request.TopicData.resize(1);
    request.TopicData[0].Name = topicName;
    request.TopicData[0].PartitionData.resize(msgs.size());
    for(size_t i = 0 ; i < msgs.size(); ++i) {
        request.TopicData[0].PartitionData[i].Index = msgs[i].first;
        request.TopicData[0].PartitionData[i].Records = msgs[i].second;
    }

    if (transactionalId) {
        request.TransactionalId = *transactionalId;
    }

    Write(So, &header, &request, false);
}

TMessagePtr<TProduceResponseData> TKafkaTestClient::ReadLastResult(i32 customCorrelationId) {
    TRequestHeaderData header = Header(NKafka::EApiKey::PRODUCE, 9, customCorrelationId);
    return Read<TProduceResponseData>(Si, &header);
}

TMessagePtr<TProduceResponseData> TKafkaTestClient::Produce(const TTopicPartition& topicPartition,
                                                            const std::vector<std::pair<TString, TString>>& keyValueMessages,
                                                            ui32 baseSequence,
                                                            const std::optional<TProducerInstanceId>& producerInstanceId,
                                                            const std::optional<TString>& transactionalId) {
    TKafkaRecordBatch batch;
    batch.BaseSequence = baseSequence;
    batch.Magic = TKafkaRecordBatch::MagicMeta::Default;
    batch.Records.resize(keyValueMessages.size());
    for (ui32 i = 0; i < keyValueMessages.size(); i++) {
        auto& keyValueMessage = keyValueMessages[i];
        batch.Records[i].Key = ToRawBytes(keyValueMessage.first);
        batch.Records[i].Value = ToRawBytes(keyValueMessage.second);
    }
    if (producerInstanceId) {
        batch.ProducerId = producerInstanceId->Id;
        batch.ProducerEpoch = producerInstanceId->Epoch;
    }
    std::vector<std::pair<ui32, TKafkaRecordBatch>> msgs;
    msgs.emplace_back(topicPartition.PartitionId, std::move(batch));
    return Produce(topicPartition.TopicPath, msgs, transactionalId);
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

    protocol.Metadata = TKafkaRawBytes(buf.GetFrontBuffer().data(), buf.GetFrontBuffer().size());

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
            syncAssignment.AssignmentStr = TString(buf.GetFrontBuffer().data(), buf.GetFrontBuffer().size());
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

TMessagePtr<TOffsetFetchResponseData> TKafkaTestClient::OffsetFetch(TOffsetFetchRequestData& request) {
    Cerr << ">>>>> TOffsetFetchRequestData\n";
    TRequestHeaderData header = Header(NKafka::EApiKey::OFFSET_FETCH, 8);
    return WriteAndRead<TOffsetFetchResponseData>(header, request);
}

TMessagePtr<TListGroupsResponseData> TKafkaTestClient::ListGroups(TListGroupsRequestData request) {
    Cerr << ">>>>> TListGroupsResponseData\n";
    TRequestHeaderData header = Header(NKafka::EApiKey::LIST_GROUPS, 4);
    return WriteAndRead<TListGroupsResponseData>(header, request);
}

TMessagePtr<TListGroupsResponseData> TKafkaTestClient::ListGroups(const std::vector<std::optional<TString>>& statesFilter) {
    Cerr << ">>>>> TListGroupsResponseData\n";
    TRequestHeaderData header = Header(NKafka::EApiKey::LIST_GROUPS, 4);
    TListGroupsRequestData request;
    request.StatesFilter = statesFilter;
    return WriteAndRead<TListGroupsResponseData>(header, request);
}

TMessagePtr<TDescribeGroupsResponseData> TKafkaTestClient::DescribeGroups(TDescribeGroupsRequestData& request) {
    Cerr << ">>>>> TDescribeGroupsResponseData\n";
    TRequestHeaderData header = Header(NKafka::EApiKey::DESCRIBE_GROUPS, 5);
    return WriteAndRead<TDescribeGroupsResponseData>(header, request);
}

TMessagePtr<TDescribeGroupsResponseData> TKafkaTestClient::DescribeGroups(const std::vector<std::optional<TString>>& groups) {
    Cerr << ">>>>> TDescribeGroupsResponseData\n";
    TRequestHeaderData header = Header(NKafka::EApiKey::DESCRIBE_GROUPS, 5);
    TDescribeGroupsRequestData request;
    request.Groups = groups;
    return WriteAndRead<TDescribeGroupsResponseData>(header, request);
}

TMessagePtr<TFetchResponseData> TKafkaTestClient::Fetch(const std::vector<std::pair<TString, std::vector<i32>>>& topics, i64 offset) {
    Cerr << ">>>>> TFetchRequestData\n";

    TRequestHeaderData header = Header(NKafka::EApiKey::FETCH, 3);

    TFetchRequestData request;
    request.MaxWaitMs = 1000;
    request.MinBytes = 1;
    request.ReplicaId = -1;

    for (auto& topic: topics) {
        NKafka::TFetchRequestData::TFetchTopic topicReq {};
        topicReq.Topic = topic.first;
        for (auto& partition: topic.second) {
            NKafka::TFetchRequestData::TFetchTopic::TFetchPartition partitionReq {};
            partitionReq.FetchOffset = offset;
            partitionReq.Partition = partition;
            partitionReq.PartitionMaxBytes = 1_MB;
            topicReq.Partitions.push_back(partitionReq);
        }
        request.Topics.push_back(topicReq);
    }

    return WriteAndRead<TFetchResponseData>(header, request);
}

TMessagePtr<TFetchResponseData> TKafkaTestClient::Fetch(const std::vector<std::pair<TKafkaUuid, std::vector<i32>>>& topics, i64 offset) {
    Cerr << ">>>>> TFetchRequestData\n";

    TRequestHeaderData header = Header(NKafka::EApiKey::FETCH, 13);

    TFetchRequestData request;
    request.MaxWaitMs = 1000;
    request.MinBytes = 1;
    request.ReplicaId = -1;

    for (auto& topic: topics) {
        NKafka::TFetchRequestData::TFetchTopic topicReq {};
        topicReq.TopicId = topic.first;
        for (auto& partition: topic.second) {
            NKafka::TFetchRequestData::TFetchTopic::TFetchPartition partitionReq {};
            partitionReq.FetchOffset = offset;
            partitionReq.Partition = partition;
            partitionReq.PartitionMaxBytes = 1_MB;
            topicReq.Partitions.push_back(partitionReq);
        }
        request.Topics.push_back(topicReq);
    }

    return WriteAndRead<TFetchResponseData>(header, request);
}

void TKafkaTestClient::ValidateNoDataInTopics(const std::vector<std::pair<TString, std::vector<i32>>>& topics, i64 offset) {
    auto fetchResponse = Fetch(topics, offset);
    UNIT_ASSERT_VALUES_EQUAL(fetchResponse->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
    for (ui32 topicIndex = 0; topicIndex < topics.size(); topicIndex++) {
        for (ui32 partitionIndex = 0; partitionIndex < topics[topicIndex].second.size(); partitionIndex++) {
            UNIT_ASSERT(!fetchResponse->Responses[topicIndex].Partitions[partitionIndex].Records.has_value());
        }
    }
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
        topic.ReplicationFactor = topicToCreate.ReplicationFactor;

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

TMessagePtr<TCreatePartitionsResponseData> TKafkaTestClient::CreatePartitions(const std::vector<TTopicConfig>& topicsToCreate, bool validateOnly) {
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
        addConfig(topicToModify.TimestampType, "message.timestamp.type");

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

TMessagePtr<TDescribeConfigsResponseData> TKafkaTestClient::DescribeConfigs(std::vector<TString> topics) {
    Cerr << ">>>>> TDescribeConfigsRequestData\n";

    TRequestHeaderData header = Header(NKafka::EApiKey::DESCRIBE_CONFIGS, 2);
    TDescribeConfigsRequestData request;

    for (auto& topic : topics) {
        NKafka::TDescribeConfigsRequestData::TDescribeConfigsResource resource;
        resource.ResourceType = TOPIC_RESOURCE_TYPE;
        resource.ResourceName = topic;
        request.Resources.push_back(resource);
    }

    return WriteAndRead<TDescribeConfigsResponseData>(header, request);
}

TMessagePtr<TAddPartitionsToTxnResponseData> TKafkaTestClient::AddPartitionsToTxn(const TString& transactionalId, const TProducerInstanceId& producerInstanceId, const std::unordered_map<TString, std::vector<ui32>>& topicPartitions) {
    TRequestHeaderData header = Header(EApiKey::ADD_PARTITIONS_TO_TXN, 3);
    TAddPartitionsToTxnRequestData request;
    request.TransactionalId = transactionalId;
    request.ProducerId = producerInstanceId.Id;
    request.ProducerEpoch = producerInstanceId.Epoch;
    for (auto& [topicName, partitions] : topicPartitions) {
        NKafka::TAddPartitionsToTxnRequestData::TAddPartitionsToTxnTopic topic;
        topic.Name = topicName;
        topic.Partitions.reserve(partitions.size());
        for (auto part : partitions) {
            topic.Partitions.push_back(part);
        }
        request.Topics.push_back(topic);
    }

    return WriteAndRead<TAddPartitionsToTxnResponseData>(header, request);
}

TMessagePtr<TAddOffsetsToTxnResponseData> TKafkaTestClient::AddOffsetsToTxn(const TString& transactionalId, const TProducerInstanceId& producerInstanceId, const TString& groupId) {
    TRequestHeaderData header = Header(EApiKey::ADD_OFFSETS_TO_TXN, 3);
    TAddOffsetsToTxnRequestData request;
    request.TransactionalId = transactionalId;
    request.ProducerId = producerInstanceId.Id;
    request.ProducerEpoch = producerInstanceId.Epoch;
    request.GroupId = groupId;

    return WriteAndRead<TAddOffsetsToTxnResponseData>(header, request);
}

TMessagePtr<TTxnOffsetCommitResponseData> TKafkaTestClient::TxnOffsetCommit(const TString& transactionalId, const TProducerInstanceId& producerInstanceId, const TString& groupName, ui32 generation, const std::unordered_map<TString, std::vector<std::pair<ui32, ui64>>>& paritionOffsetsToTopic) {
    TRequestHeaderData header = Header(EApiKey::TXN_OFFSET_COMMIT, 3);
    TTxnOffsetCommitRequestData request;
    request.TransactionalId = transactionalId;
    request.ProducerId = producerInstanceId.Id;
    request.ProducerEpoch = producerInstanceId.Epoch;
    request.GroupId = groupName;
    request.GenerationId = generation;
    for (auto& [topicName, partitionsAndOffsets] : paritionOffsetsToTopic) {
        NKafka::TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic topic;
        topic.Name = topicName;
        topic.Partitions.reserve(partitionsAndOffsets.size());
        for (auto partitionAndOffset : partitionsAndOffsets) {
            NKafka::TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic::TTxnOffsetCommitRequestPartition partition;
            partition.PartitionIndex = partitionAndOffset.first;
            partition.CommittedOffset = partitionAndOffset.second;
            topic.Partitions.push_back(partition);
        }
        request.Topics.push_back(topic);
    }

    return WriteAndRead<TTxnOffsetCommitResponseData>(header, request);
}

TMessagePtr<TEndTxnResponseData> TKafkaTestClient::EndTxn(const TString& transactionalId, const TProducerInstanceId& producerInstanceId, bool commit) {
    TRequestHeaderData header = Header(EApiKey::END_TXN, 3);
    TEndTxnRequestData request;
    request.TransactionalId = transactionalId;
    request.ProducerId = producerInstanceId.Id;
    request.ProducerEpoch = producerInstanceId.Epoch;
    request.Committed = commit;

    return WriteAndRead<TEndTxnResponseData>(header, request);
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

void TKafkaTestClient::PlainAuthenticateToKafka() {
    PlainAuthenticateToKafka("ouruser@/Root", "ourUserPassword");
}

void TKafkaTestClient::PlainAuthenticateToKafka(const TString& userName, const TString& userPassword) {
    {
        auto msg = ApiVersions();

        UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(msg->ApiKeys.size(), EXPECTED_API_KEYS_COUNT);
    }

    {
        auto msg = SaslHandshake();

        UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(msg->Mechanisms.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(*msg->Mechanisms[0], "PLAIN");
        UNIT_ASSERT_VALUES_EQUAL(*msg->Mechanisms[1], "SCRAM-SHA-256");
    }

    {
        auto msg = SaslPlainAuthenticate(userName, userPassword);
        UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
    }

}

void TKafkaTestClient::ScramAuthenticateToKafka() {
    ScramAuthenticateToKafka("ouruser", "ourUserPassword");
}

void TKafkaTestClient::ScramAuthenticateToKafka(const TString& userName, const TString& userPassword) {
    {
        auto msg = ApiVersions();

        UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(msg->ApiKeys.size(), EXPECTED_API_KEYS_COUNT);
    }

    {
        auto msg = SaslHandshake("SCRAM-SHA-256");

        UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(msg->Mechanisms.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(*msg->Mechanisms[0], "PLAIN");
        UNIT_ASSERT_VALUES_EQUAL(*msg->Mechanisms[1], "SCRAM-SHA-256");
    }

    // Generate random client nonce (16 random bytes encoded as base64)
    TString randomBytes;
    randomBytes.reserve(16);
    for (size_t i = 0; i < 16; ++i) {
        randomBytes += static_cast<char>(RandomNumber<ui8>());
    }
    TString clientNonce = Base64Encode(randomBytes);

    // Build first client message bare for auth message computation
    TString clientFirstMessageBare = TStringBuilder() << "n=" << userName << ",r=" << clientNonce;

    // Send first SCRAM message
    auto response1 = SaslScramAuthenticateFirstMsg(userName, clientNonce);

    // Check for errors in the first response
    if (response1->ErrorCode != static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR)) {
        // Try to parse as error message
        std::string errorMessage(response1->AuthBytes->data(), response1->AuthBytes->size());
        NLogin::NSasl::TFinalServerMsg errorMsg;
        auto parseErrorResult = NLogin::NSasl::ParseFinalServerMsg(errorMessage, errorMsg);
        if (parseErrorResult == NLogin::NSasl::EParseMsgReturnCodes::Success && !errorMsg.Error.empty()) {
            UNIT_FAIL("SCRAM authentication failed with error: " + TString(errorMsg.Error));
        } else {
            UNIT_FAIL("SCRAM authentication failed with error code: " + ToString(response1->ErrorCode));
        }
    }

    // Parse server first message
    UNIT_ASSERT(response1->AuthBytes.has_value());
    const auto& authBytes1 = response1->AuthBytes.value();
    std::string serverFirstMessage(reinterpret_cast<const char*>(authBytes1.data()), authBytes1.size());

    NLogin::NSasl::TFirstServerMsg parsedServerMsg;
    auto parseResult = NLogin::NSasl::ParseFirstServerMsg(serverFirstMessage, parsedServerMsg);
    UNIT_ASSERT_C(parseResult == NLogin::NSasl::EParseMsgReturnCodes::Success, "Failed to parse server first message");

    // Decode salt from base64
    TString decodedSalt = Base64StrictDecode(parsedServerMsg.Salt);

    // Build client final message without proof
    TString gs2Header = "n,,"; // No channel binding, no authzid
    TString channelBinding = Base64Encode(gs2Header);
    TString clientFinalMessageWithoutProof = TStringBuilder() << "c=" << channelBinding << ",r=" << parsedServerMsg.Nonce;

    // Compute auth message
    TString authMessage = clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof;

    // Compute client proof
    std::string clientProof;
    std::string errorText;
    bool success = NLogin::NSasl::ComputeClientProof(
        "SCRAM-SHA-256",
        std::string(userPassword.data(), userPassword.size()),
        std::string(decodedSalt.data(), decodedSalt.size()),
        parsedServerMsg.IterationsCount,
        std::string(authMessage.data(), authMessage.size()),
        clientProof,
        errorText
    );
    UNIT_ASSERT_C(success, TString(errorText));

    // Encode client proof to base64
    TString encodedClientProof = Base64Encode(clientProof);

    // Send final SCRAM message
    auto response2 = SaslScramAuthenticateFinalMsg(TString(parsedServerMsg.Nonce), encodedClientProof);
    UNIT_ASSERT_VALUES_EQUAL(response2->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

    // Parse server final message to verify server signature
    UNIT_ASSERT(response2->AuthBytes.has_value());
    const auto& authBytes2 = response2->AuthBytes.value();
    std::string serverFinalMessage(reinterpret_cast<const char*>(authBytes2.data()), authBytes2.size());

    Cerr << ">>>>> Server final message: " << serverFinalMessage << Endl;
    Cerr << ">>>>> Server final message length: " << serverFinalMessage.size() << Endl;

    // DEBUG: Print raw bytes
    Cerr << ">>>>> Raw bytes: ";
    for (size_t i = 0; i < authBytes2.size(); ++i) {
        Cerr << "0x" << Hex((unsigned char)authBytes2[i]) << " ";
    }
    Cerr << Endl;

    NLogin::NSasl::TFinalServerMsg parsedFinalServerMsg;
    auto parseFinalResult = NLogin::NSasl::ParseFinalServerMsg(serverFinalMessage, parsedFinalServerMsg);
    UNIT_ASSERT_C(parseFinalResult == NLogin::NSasl::EParseMsgReturnCodes::Success,
        "Failed to parse server final message. Message: '" + TString(serverFinalMessage) + "'");

    // Check if there's an error in the server response
    UNIT_ASSERT_C(parsedFinalServerMsg.Error.empty(), "Server returned error: " + TString(parsedFinalServerMsg.Error));

    // Verify server signature
    UNIT_ASSERT_C(!parsedFinalServerMsg.ServerSignature.empty(), "Server signature is empty");

    // Compute expected server signature
    std::string serverKey;
    std::string errorText2;
    bool success2 = NLogin::NSasl::ComputeServerKey(
        "SCRAM-SHA-256",
        std::string(userPassword.data(), userPassword.size()),
        std::string(decodedSalt.data(), decodedSalt.size()),
        parsedServerMsg.IterationsCount,
        serverKey,
        errorText2
    );
    UNIT_ASSERT_C(success2, TString(errorText2));

    std::string expectedServerSignature;
    success2 = NLogin::NSasl::ComputeServerSignature(
        "SCRAM-SHA-256",
        serverKey,
        std::string(authMessage.data(), authMessage.size()),
        expectedServerSignature,
        errorText2
    );
    UNIT_ASSERT_C(success2, TString(errorText2));

    // Decode received server signature from base64
    TString decodedServerSignature = Base64StrictDecode(parsedFinalServerMsg.ServerSignature);

    // Compare signatures
    UNIT_ASSERT_VALUES_EQUAL_C(
        TString(expectedServerSignature.data(), expectedServerSignature.size()),
        decodedServerSignature,
        "Server signature verification failed"
    );
}

TRequestHeaderData TKafkaTestClient::Header(NKafka::EApiKey apiKey, TKafkaVersion version, i32 customCorrelationId) {
    TRequestHeaderData header;
    header.RequestApiKey = apiKey;
    header.RequestApiVersion = version;
    if (customCorrelationId == -1) {
        header.CorrelationId = NextCorrelation();
    } else {
        header.CorrelationId = customCorrelationId;
    }
    header.ClientId = ClientName;
    return header;
}

ui32 TKafkaTestClient::NextCorrelation() {
    return Correlation++;
}

template <std::derived_from<TApiMessage> T>
TMessagePtr<T> TKafkaTestClient::WriteAndRead(TRequestHeaderData& header, TApiMessage& request, bool silent) {
    Write(So, &header, &request, silent);
    return Read<T>(Si, &header);
}

void TKafkaTestClient::Write(TSocketOutput& so, TApiMessage* request, TKafkaVersion version, bool silent) {
    TWritableBuf sb(nullptr, request->Size(version) + 1000);
    TKafkaWritable writable(sb);
    request->Write(writable, version);
    so.Write(sb.GetFrontBuffer().data(), sb.GetFrontBuffer().size());

    if (!silent) {
        Print(sb.GetFrontBuffer());
    }
}

void TKafkaTestClient::Write(TSocketOutput& so, TRequestHeaderData* header, TApiMessage* request, bool silent) {
    TKafkaVersion version = header->RequestApiVersion;
    TKafkaVersion headerVersion = RequestHeaderVersion(request->ApiKey(), version);

    TKafkaInt32 size = header->Size(headerVersion) + request->Size(version);
    if (!silent) {
        Cerr << ">>>>> Size=" << size << Endl;
    }
    NKafka::NormalizeNumber(size);
    so.Write(&size, sizeof(size));

    Write(so, header, headerVersion, silent);
    Write(so, request, version, silent);

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
    Cerr << ">>>>> Packet sent: " << Hex(buffer.Begin(), buffer.End()) << Endl;
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

template
TMessagePtr<TProduceResponseData> TKafkaTestClient::WriteAndRead<TProduceResponseData>(TRequestHeaderData& header, TApiMessage& request, bool silent = false);

template
TMessagePtr<TProduceResponseData> TKafkaTestClient::Read<TProduceResponseData>(TSocketInput& si, TRequestHeaderData* requestHeader);
