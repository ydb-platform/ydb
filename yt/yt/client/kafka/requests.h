#pragma once

#include "public.h"

#include "protocol.h"

#include <util/generic/guid.h>

namespace NYT::NKafka {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ERequestType,
    ((None)               (-1))
    ((Fetch)              (1))
    ((ListOffsets)        (2)) // Unimplemented.
    ((Metadata)           (3))
    ((UpdateMetadata)     (6)) // Unimplemented.
    ((OffsetCommit)       (8)) // Unimplemented.
    ((OffsetFetch)        (9))
    ((FindCoordinator)    (10))
    ((JoinGroup)          (11)) // Unimplemented.
    ((Heartbeat)          (12)) // Unimplemented.
    ((SyncGroup)          (14)) // Unimplemented.
    ((DescribeGroups)     (15)) // Unimplemented.
    ((SaslHandshake)      (17))
    ((ApiVersions)        (18)) // Unimplemented.
    ((SaslAuthenticate)   (36)) // Unimplemented.
);

////////////////////////////////////////////////////////////////////////////////

struct TRequestHeader
{
    ERequestType RequestType;
    int16_t ApiVersion = 0;
    int32_t CorrelationId = 0;
    TString ClientId;

    void Deserialize(IKafkaProtocolReader* reader);

private:
    int GetVersion();
};

struct TResponseHeader
{
    int32_t CorrelationId = 0;

    void Serialize(IKafkaProtocolWriter* writer);
};

////////////////////////////////////////////////////////////////////////////////

struct TReqApiVersions
{
    TString ClientSoftwareName;
    TString ClientSoftwareVersion;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);

    static ERequestType GetRequestType()
    {
        return ERequestType::ApiVersions;
    }
};

struct TRspApiKey
{
    int16_t ApiKey = -1;
    int16_t MinVersion = 0;
    int16_t MaxVersion = 0;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

struct TRspApiVersions
{
    int16_t ErrorCode = 0;
    std::vector<TRspApiKey> ApiKeys;
    int32_t ThrottleTimeMs = 0;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TReqMetadataTopic
{
    TGUID TopicId;
    TString Topic;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TReqMetadata
{
    std::vector<TReqMetadataTopic> Topics;
    bool AllowAutoTopicCreation;
    bool IncludeClusterAuthorizedOperations;
    bool IncludeTopicAuthorizedOperations;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);

    static ERequestType GetRequestType()
    {
        return ERequestType::Metadata;
    }
};

struct TRspMetadataBroker
{
    int32_t NodeId = 0;
    TString Host;
    int32_t Port = 0;
    TString Rack;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

struct TRspMetadataTopicPartition
{
    int16_t ErrorCode = 0;

    int32_t PartitionIndex = 0;
    int32_t LeaderId = 0;
    int32_t LeaderEpoch = 0;
    std::vector<int32_t> ReplicaNodes;
    std::vector<int32_t> IsrNodes;
    std::vector<int32_t> OfflineReplicas;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

struct TRspMetadataTopic
{
    int16_t ErrorCode = 0;
    TString Name;
    TGUID TopicId;
    bool IsInternal = false;
    std::vector<TRspMetadataTopicPartition> Partitions;
    int32_t TopicAuthorizedOperations = 0;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

struct TRspMetadata
{
    int32_t ThrottleTimeMs = 0;
    std::vector<TRspMetadataBroker> Brokers;
    int32_t ClusterId = 0;
    int32_t ControllerId = 0;
    std::vector<TRspMetadataTopic> Topics;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TReqFindCoordinator
{
    TString Key;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);

    static ERequestType GetRequestType()
    {
        return ERequestType::FindCoordinator;
    }
};

struct TRspFindCoordinator
{
    int16_t ErrorCode = 0;
    int32_t NodeId = 0;
    TString Host;
    int32_t Port = 0;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TReqJoinGroupProtocol
{
    TString Name;
    TString Metadata; // TODO(nadya73): bytes.

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TReqJoinGroup
{
    TString GroupId;
    int32_t SessionTimeoutMs = 0;
    TString MemberId;
    TString ProtocolType;
    std::vector<TReqJoinGroupProtocol> Protocols;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);

    static ERequestType GetRequestType()
    {
        return ERequestType::JoinGroup;
    }
};

struct TRspJoinGroupMember
{
    TString MemberId;
    TString Metadata; // TODO(nadya73): bytes.

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

struct TRspJoinGroup
{
    int16_t ErrorCode = 0;
    int32_t GenerationId = 0;
    TString ProtocolName;
    TString Leader;
    TString MemberId;
    std::vector<TRspJoinGroupMember> Members;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TReqSyncGroupAssignment
{
    TString MemberId;
    TString Assignment;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TReqSyncGroup
{
    TString GroupId;
    TString GenerationId;
    TString MemberId;
    std::vector<TReqSyncGroupAssignment> Assignments;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);

    static ERequestType GetRequestType()
    {
        return ERequestType::SyncGroup;
    }
};

struct TRspSyncGroupAssignment
{
    TString Topic;
    std::vector<int32_t> Partitions;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

struct TRspSyncGroup
{
    int16_t ErrorCode = 0;
    std::vector<TRspSyncGroupAssignment> Assignments;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TReqHeartbeat
{
    TString GroupId;
    int32_t GenerationId = 0;
    TString MemberId;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);

    static ERequestType GetRequestType()
    {
        return ERequestType::Heartbeat;
    }
};

struct TRspHeartbeat
{
    int16_t ErrorCode = 0;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TReqOffsetFetchTopic
{
    TString Name;
    std::vector<int32_t> PartitionIndexes;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TReqOffsetFetch
{
    TString GroupId;
    std::vector<TReqOffsetFetchTopic> Topics;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);

    static ERequestType GetRequestType()
    {
        return ERequestType::OffsetFetch;
    }
};

struct TRspOffsetFetchTopicPartition
{
    int32_t PartitionIndex = 0;
    int64_t CommittedOffset = 0;
    std::optional<TString> Metadata;
    int16_t ErrorCode;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

struct TRspOffsetFetchTopic
{
    TString Name;
    std::vector<TRspOffsetFetchTopicPartition> Partitions;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

struct TRspOffsetFetch
{
    std::vector<TRspOffsetFetchTopic> Topics;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TReqFetchTopicPartition
{
    int32_t Partition = 0;
    int64_t FetchOffset = 0;
    int32_t PartitionMaxBytes = 0;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TReqFetchTopic
{
    TString Topic;
    std::vector<TReqFetchTopicPartition> Partitions;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TReqFetch
{
    int32_t ReplicaId = 0;
    int32_t MaxWaitMs = 0;
    int32_t MinBytes = 0;
    std::vector<TReqFetchTopic> Topics;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);

    static ERequestType GetRequestType()
    {
        return ERequestType::Fetch;
    }
};

struct TMessage
{
    int32_t Crc = 0;
    int8_t MagicByte = 0;
    int8_t Attributes = 0;
    TString Key;
    TString Value;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

struct TRecord
{
    int64_t Offset = 0;
    int32_t BatchSize = 0;
    TMessage Message;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

struct TRspFetchResponsePartition
{
    int32_t PartitionIndex = 0;
    int16_t ErrorCode = 0;
    int64_t HighWatermark = 0;
    std::optional<std::vector<TRecord>> Records;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

struct TRspFetchResponse
{
    TString Topic;
    std::vector<TRspFetchResponsePartition> Partitions;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

struct TRspFetch
{
    std::vector<TRspFetchResponse> Responses;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TReqSaslHandshake
{
    TString Mechanism;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);

    static ERequestType GetRequestType()
    {
        return ERequestType::SaslHandshake;
    }
};

struct TRspSaslHandshake
{
    NKafka::EErrorCode ErrorCode = EErrorCode::None;
    std::vector<TString> Mechanisms;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TReqSaslAuthenticate
{
    TString AuthBytes;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);

    static ERequestType GetRequestType()
    {
        return ERequestType::SaslAuthenticate;
    }
};

struct TRspSaslAuthenticate
{
    EErrorCode ErrorCode = EErrorCode::None;
    std::optional<TString> ErrorMessage;
    TString AuthBytes;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafka
