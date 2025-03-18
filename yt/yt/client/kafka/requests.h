#pragma once

#include "public.h"

#include "protocol.h"

#include <library/cpp/yt/misc/guid.h>

namespace NYT::NKafka {

////////////////////////////////////////////////////////////////////////////////

using TMemberId = TString;
using TGroupId = TString;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ERequestType,
    ((None)               (-1))
    ((Produce)            (0))
    ((Fetch)              (1))
    ((ListOffsets)        (2))
    ((Metadata)           (3))
    ((UpdateMetadata)     (6)) // Unimplemented.
    ((OffsetCommit)       (8))
    ((OffsetFetch)        (9))
    ((FindCoordinator)    (10))
    ((JoinGroup)          (11))
    ((Heartbeat)          (12))
    ((LeaveGroup)         (13))
    ((SyncGroup)          (14))
    ((DescribeGroups)     (15)) // Unimplemented.
    ((SaslHandshake)      (17))
    ((ApiVersions)        (18))
    ((SaslAuthenticate)   (36)) // Unimplemented.
);

////////////////////////////////////////////////////////////////////////////////

struct TTaggedField
{
    ui32 Tag = 0;
    TString Data;

    void Serialize(IKafkaProtocolWriter* writer) const;
    void Deserialize(IKafkaProtocolReader* reader);
};

////////////////////////////////////////////////////////////////////////////////

int GetRequestHeaderVersion(ERequestType requestType, i16 apiVersion);
int GetResponseHeaderVersion(ERequestType requestType, i16 apiVersion);

struct TRequestHeader
{
    ERequestType RequestType;
    i16 ApiVersion = 0;
    i32 CorrelationId = 0;

    // Present in v1 and v2.
    std::optional<TString> ClientId;

    // Present in v2 only.
    std::vector<TTaggedField> TagBuffer;

    void Deserialize(IKafkaProtocolReader* reader);
};

struct TResponseHeader
{
    i32 CorrelationId = 0;

     // Present in v1 only.
    std::vector<TTaggedField> TagBuffer;

    void Serialize(IKafkaProtocolWriter* writer, int version);
};

////////////////////////////////////////////////////////////////////////////////

struct TRecordHeader
{
    TString HeaderKey;
    TString HeaderValue;

    void Serialize(IKafkaProtocolWriter* writer, int version) const;
    void Deserialize(IKafkaProtocolReader* reader, int version);
};

struct TRecord
{
    // Present in v1 and v2.
    i8 Attributes = 0;

    // Present in v2 only.
    i32 TimestampDelta = 0;
    i32 OffsetDelta = 0;

    // Present in v1 and v2.
    TString Key;
    TString Value;

    std::vector<TRecordHeader> Headers;

    void Serialize(IKafkaProtocolWriter* writer, int version) const;
    void Deserialize(IKafkaProtocolReader* reader, int version);
};

// Same as MessageSet.
struct TRecordBatch
{
    // Present in v1 and v2.
    // Same as Offset in v1 and BaseOffset in v2.
    i64 BaseOffset = 0;
    // Same as MessageSize in v1 and BatchLength in v2.
    i32 Length = 0;

    i32 PartitionLeaderEpoch = 0;

    i8 MagicByte = 0;

    // Present in Message (for v1) or in MessageSet (for v2).
    i32 CrcOld = 0;
    ui32 Crc = 0;

    // Present in v2 only.
    i16 Attributes = 0;

    i32 LastOffsetDelta = 0;
    // BaseTimestamp in v2 and ... TODO in v1.
    i64 FirstTimestamp = -1;
    i64 MaxTimestamp = -1;

    i64 ProducerId = -1;
    i16 ProducerEpoch = -1;
    // Same as BaseSequence in v2 and TODO.
    i32 BaseSequence = 0;

    // Always one record (for v1) or several records (for v2).
    std::vector<TRecord> Records;

    void Serialize(IKafkaProtocolWriter* writer) const;
    void Deserialize(IKafkaProtocolReader* reader);
};

////////////////////////////////////////////////////////////////////////////////

struct TReqBase
{
    int ApiVersion = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TReqApiVersions
{
    static constexpr ERequestType RequestType = ERequestType::ApiVersions;

    TString ClientSoftwareName;
    TString ClientSoftwareVersion;
    std::vector<TTaggedField> TagBuffer;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TRspApiKey
{
    i16 ApiKey = -1;
    i16 MinVersion = 0;
    i16 MaxVersion = 0;
    std::vector<TTaggedField> TagBuffer;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

struct TRspApiVersions
{
    NKafka::EErrorCode ErrorCode = NKafka::EErrorCode::None;
    std::vector<TRspApiKey> ApiKeys;
    i32 ThrottleTimeMs = 0;
    std::vector<TTaggedField> TagBuffer;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TReqMetadataTopic
{
    TGuid TopicId;
    TString Topic;
    std::vector<TTaggedField> TagBuffer;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TReqMetadata
{
    static constexpr ERequestType RequestType = ERequestType::Metadata;

    std::vector<TReqMetadataTopic> Topics;
    bool AllowAutoTopicCreation;
    bool IncludeClusterAuthorizedOperations;
    bool IncludeTopicAuthorizedOperations;
    std::vector<TTaggedField> TagBuffer;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TRspMetadataBroker
{
    i32 NodeId = 0;
    TString Host;
    i32 Port = 0;
    std::optional<TString> Rack;
    std::vector<TTaggedField> TagBuffer;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

struct TRspMetadataTopicPartition
{
    NKafka::EErrorCode ErrorCode = NKafka::EErrorCode::None;

    i32 PartitionIndex = 0;
    i32 LeaderId = 0;
    i32 LeaderEpoch = 0;
    std::vector<i32> ReplicaNodes;
    std::vector<i32> IsrNodes;
    std::vector<i32> OfflineReplicas;
    std::vector<TTaggedField> TagBuffer;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

struct TRspMetadataTopic
{
    NKafka::EErrorCode ErrorCode = NKafka::EErrorCode::None;
    TString Name;
    TGuid TopicId;
    bool IsInternal = false;
    std::vector<TRspMetadataTopicPartition> Partitions;
    i32 TopicAuthorizedOperations = 0;
    std::vector<TTaggedField> TagBuffer;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

struct TRspMetadata
{
    i32 ThrottleTimeMs = 0;
    std::vector<TRspMetadataBroker> Brokers;
    std::optional<TString> ClusterId;
    i32 ControllerId = 0;
    std::vector<TRspMetadataTopic> Topics;
    std::vector<TTaggedField> TagBuffer;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TReqFindCoordinator
{
    static constexpr ERequestType RequestType = ERequestType::FindCoordinator;

    TString Key;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TRspFindCoordinator
{
    NKafka::EErrorCode ErrorCode = NKafka::EErrorCode::None;
    i32 NodeId = 0;
    TString Host;
    i32 Port = 0;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TReqJoinGroupProtocol
{
    TString Name;
    TString Metadata;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TReqJoinGroup
{
    static constexpr ERequestType RequestType = ERequestType::JoinGroup;

    TGroupId GroupId;
    i32 SessionTimeoutMs = 0;
    TMemberId MemberId;
    TString ProtocolType;
    std::vector<TReqJoinGroupProtocol> Protocols;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TRspJoinGroupMember
{
    TMemberId MemberId;
    TString Metadata; // TODO(nadya73): bytes.

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

struct TRspJoinGroup
{
    NKafka::EErrorCode ErrorCode = NKafka::EErrorCode::None;
    i32 GenerationId = 0;
    TString ProtocolName;
    TString Leader;
    TMemberId MemberId;
    std::vector<TRspJoinGroupMember> Members;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TReqSyncGroupAssignment
{
    TMemberId MemberId;
    TString Assignment;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TReqSyncGroup
{
    static constexpr ERequestType RequestType = ERequestType::SyncGroup;

    TGroupId GroupId;
    i32 GenerationId = 0;
    TMemberId MemberId;
    std::vector<TReqSyncGroupAssignment> Assignments;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TRspSyncGroup
{
    NKafka::EErrorCode ErrorCode = NKafka::EErrorCode::None;
    TString Assignment;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TReqHeartbeat
{
    static constexpr ERequestType RequestType = ERequestType::Heartbeat;

    TGroupId GroupId;
    i32 GenerationId = 0;
    TMemberId MemberId;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TRspHeartbeat
{
    NKafka::EErrorCode ErrorCode = NKafka::EErrorCode::None;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TReqLeaveGroup
{
    static constexpr ERequestType RequestType = ERequestType::LeaveGroup;

    TGroupId GroupId;
    TMemberId MemberId;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TRspLeaveGroup
{
    NKafka::EErrorCode ErrorCode = NKafka::EErrorCode::None;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TReqOffsetCommitTopicPartition
{
    i32 PartitionIndex = 0;
    i64 CommittedOffset = 0;
    std::optional<TString> CommittedMetadata;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TReqOffsetCommitTopic
{
    TString Name;
    std::vector<TReqOffsetCommitTopicPartition> Partitions;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TReqOffsetCommit
{
    static constexpr ERequestType RequestType = ERequestType::OffsetCommit;

    TString GroupId;
    std::vector<TReqOffsetCommitTopic> Topics;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TRspOffsetCommitTopicPartition
{
    i32 PartitionIndex = 0;
    NKafka::EErrorCode ErrorCode = NKafka::EErrorCode::None;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

struct TRspOffsetCommitTopic
{
    TString Name;
    std::vector<TRspOffsetCommitTopicPartition> Partitions;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

struct TRspOffsetCommit
{
    std::vector<TRspOffsetCommitTopic> Topics;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TReqOffsetFetchTopic
{
    TString Name;
    std::vector<i32> PartitionIndexes;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TReqOffsetFetch
{
    static constexpr ERequestType RequestType = ERequestType::OffsetFetch;

    TString GroupId;
    std::vector<TReqOffsetFetchTopic> Topics;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TRspOffsetFetchTopicPartition
{
    i32 PartitionIndex = 0;
    i64 CommittedOffset = 0;
    std::optional<TString> Metadata;
    NKafka::EErrorCode ErrorCode = NKafka::EErrorCode::None;

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
    i32 Partition = 0;
    i64 FetchOffset = 0;
    i32 PartitionMaxBytes = 0;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TReqFetchTopic
{
    TString Topic;
    std::vector<TReqFetchTopicPartition> Partitions;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TReqFetch
    : public TReqBase
{
    static constexpr ERequestType RequestType = ERequestType::Fetch;

    i32 ReplicaId = 0;
    i32 MaxWaitMs = 0;
    i32 MinBytes = 0;
    i32 MaxBytes = 0;
    std::vector<TReqFetchTopic> Topics;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TRspFetchResponsePartition
{
    i32 PartitionIndex = 0;
    NKafka::EErrorCode ErrorCode = NKafka::EErrorCode::None;
    i64 HighWatermark = 0;
    std::optional<std::vector<TRecordBatch>> RecordBatches;

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
    i32 ThrottleTimeMs = 0;
    std::vector<TRspFetchResponse> Responses;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TReqSaslHandshake
    : public TReqBase
{
    static constexpr ERequestType RequestType = ERequestType::SaslHandshake;

    TString Mechanism;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TRspSaslHandshake
{
    NKafka::EErrorCode ErrorCode = NKafka::EErrorCode::None;
    std::vector<TString> Mechanisms;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TReqSaslAuthenticate
{
    static constexpr ERequestType RequestType = ERequestType::SaslAuthenticate;

    TString AuthBytes;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TRspSaslAuthenticate
{
    NKafka::EErrorCode ErrorCode = NKafka::EErrorCode::None;
    std::optional<TString> ErrorMessage;
    TString AuthBytes;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TReqProduceTopicDataPartitionData
{
    i32 Index = 0;
    std::vector<TRecordBatch> RecordBatches;
    std::vector<TTaggedField> TagBuffer;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TReqProduceTopicData
{
    TString Name;
    std::vector<TReqProduceTopicDataPartitionData> PartitionData;
    std::vector<TTaggedField> TagBuffer;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TReqProduce
{
    static constexpr ERequestType RequestType = ERequestType::Produce;

    std::optional<TString> TransactionalId;
    i16 Acks = 0;
    i32 TimeoutMs = 0;
    std::vector<TReqProduceTopicData> TopicData;
    std::vector<TTaggedField> TagBuffer;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TRspProduceResponsePartitionResponseRecordError
{
    i32 BatchIndex = 0;
    std::optional<TString> BatchIndexErrorMessage;
    std::vector<TTaggedField> TagBuffer;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

struct TRspProduceResponsePartitionResponse
{
    i32 Index = 0;
    NKafka::EErrorCode ErrorCode = NKafka::EErrorCode::None;
    i64 BaseOffset = 0;
    i64 LogAppendTimeMs = 0;
    i64 LogStartOffset = 0;
    std::vector<TRspProduceResponsePartitionResponseRecordError> RecordErrors;
    std::optional<TString> ErrorMessage;
    std::vector<TTaggedField> TagBuffer;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

struct TRspProduceResponse
{
    TString Name;
    std::vector<TRspProduceResponsePartitionResponse> PartitionResponses;
    std::vector<TTaggedField> TagBuffer;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

struct TRspProduce
{
    std::vector<TRspProduceResponse> Responses;
    i32 ThrottleTimeMs = 0;
    std::vector<TTaggedField> TagBuffer;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TReqListOffsetsTopicPartition
{
    i32 PartitionIndex = 0;
    i64 Timestamp = 0;
    i32 MaxNumOffsets = 0;

    std::vector<TTaggedField> TagBuffer;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TReqListOffsetsTopic
{
    TString Name;
    std::vector<TReqListOffsetsTopicPartition> Partitions;

    std::vector<TTaggedField> TagBuffer;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TReqListOffsets
{
    static constexpr ERequestType RequestType = ERequestType::ListOffsets;

    i32 ReplicaId = 0;
    std::vector<TReqListOffsetsTopic> Topics;

    std::vector<TTaggedField> TagBuffer;

    void Deserialize(IKafkaProtocolReader* reader, int apiVersion);
};

struct TRspListOffsetsTopicPartition
{
    i32 PartitionIndex = 0;
    NKafka::EErrorCode ErrorCode = EErrorCode::None;
    i64 Offset = 0;

    std::vector<TTaggedField> TagBuffer;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

struct TRspListOffsetsTopic
{
    TString Name;
    std::vector<TRspListOffsetsTopicPartition> Partitions;

    std::vector<TTaggedField> TagBuffer;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

struct TRspListOffsets
{
    std::vector<TRspListOffsetsTopic> Topics;

    std::vector<TTaggedField> TagBuffer;

    void Serialize(IKafkaProtocolWriter* writer, int apiVersion) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafka

#define REQUESTS_INL_H_
#include "requests-inl.h"
#undef REQUESTS_INL_H_
