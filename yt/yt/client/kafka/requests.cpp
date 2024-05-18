#include "requests.h"

namespace NYT::NKafka {

////////////////////////////////////////////////////////////////////////////////

void TRequestHeader::Deserialize(IKafkaProtocolReader *reader)
{
    auto apiKey = reader->ReadInt16();
    RequestType = static_cast<ERequestType>(apiKey);
    ApiVersion = reader->ReadInt16();
    CorrelationId = reader->ReadInt32();

    if (GetVersion() >= 1) {
        ClientId = reader->ReadString();
    }
}

int TRequestHeader::GetVersion()
{
    switch (RequestType) {
        case ERequestType::ApiVersions: {
            if (ApiVersion >= 3) {
                return 2;
            }
            return 1;
        }
        case ERequestType::Metadata: {
            if (ApiVersion >= 9) {
                return 2;
            }
            return 1;
        }
        case ERequestType::Fetch: {
            // TODO(nadya73): add version check
            return 1;
        }
        case ERequestType::None: {
            // TODO(nadya73): throw error.
            return 2;
        }
        default:
            return 2;
    }
}

void TResponseHeader::Serialize(IKafkaProtocolWriter *writer)
{
    writer->WriteInt32(CorrelationId);
}

////////////////////////////////////////////////////////////////////////////////

void TReqApiVersions::Deserialize(IKafkaProtocolReader* reader, int apiVersion)
{
    if (apiVersion <= 2) {
        return;
    }

    ClientSoftwareName = reader->ReadCompactString();
    ClientSoftwareVersion = reader->ReadCompactString();
}

void TRspApiKey::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    writer->WriteInt16(ApiKey);
    writer->WriteInt16(MinVersion);
    writer->WriteInt16(MaxVersion);

    if (apiVersion >= 3) {
        writer->WriteUnsignedVarInt(0);
        // TODO(nadya73): support tagged fields.
    }
}

void TRspApiVersions::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    writer->WriteInt16(ErrorCode);
    writer->WriteUnsignedVarInt(ApiKeys.size() + 1);
    for (const auto& apiKey :  ApiKeys) {
        apiKey.Serialize(writer, apiVersion);
    }

    if (apiVersion >= 2) {
        writer->WriteInt32(ThrottleTimeMs);
    }

    if (apiVersion >= 3) {
        writer->WriteUnsignedVarInt(0);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TReqMetadataTopic::Deserialize(IKafkaProtocolReader* reader, int apiVersion)
{
    if (apiVersion >= 10) {
        TopicId = reader->ReadUuid();
    }

    if (apiVersion < 9) {
        Topic = reader->ReadString();
    } else {
        // TODO(nadya73): handle null string.
        Topic = reader->ReadCompactString();
    }
    if (apiVersion >= 9) {
        reader->ReadUnsignedVarInt();
        // TODO(nadya73): read tagged fields.
    }
}

void TReqMetadata::Deserialize(IKafkaProtocolReader* reader, int apiVersion)
{
    // TODO(nadya73): check version and call reader->ReadUnsignedVarInt() in some cases.
    auto topicsCount = reader->ReadInt32();
    Topics.resize(topicsCount);

    for (auto& topic : Topics) {
        topic.Deserialize(reader, apiVersion);
    }

    if (apiVersion >= 4) {
        AllowAutoTopicCreation = reader->ReadBool();
    }

    if (apiVersion >= 8) {
        if (apiVersion <= 10) {
            IncludeClusterAuthorizedOperations = reader->ReadBool();
        }
        IncludeTopicAuthorizedOperations = reader->ReadBool();
    }

    if (apiVersion >= 9) {
        reader->ReadUnsignedVarInt();
        // TODO(nadya73): read tagged fields.
    }
}

void TRspMetadataBroker::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    writer->WriteInt32(NodeId);
    writer->WriteString(Host);
    writer->WriteInt32(Port);
    if (apiVersion >= 1) {
        writer->WriteString(Rack);
    }
}

void TRspMetadataTopicPartition::Serialize(IKafkaProtocolWriter* writer, int /*apiVersion*/) const
{
    writer->WriteInt16(ErrorCode);
    writer->WriteInt32(PartitionIndex);
    writer->WriteInt32(LeaderId);
    writer->WriteInt32(ReplicaNodes.size());
    for (auto replicaNode : ReplicaNodes) {
        writer->WriteInt32(replicaNode);
    }
    writer->WriteInt32(IsrNodes.size());
    for (auto isrNode : IsrNodes) {
        writer->WriteInt32(isrNode);
    }
}

void TRspMetadataTopic::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    writer->WriteInt16(ErrorCode);
    writer->WriteString(Name);
    if (apiVersion >= 1) {
        writer->WriteBool(IsInternal);
    }
    writer->WriteInt32(Partitions.size());
    for (const auto& partition : Partitions) {
        partition.Serialize(writer, apiVersion);
    }
}

void TRspMetadata::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    writer->WriteInt32(Brokers.size());
    for (const auto& broker : Brokers) {
        broker.Serialize(writer, apiVersion);
    }
    if (apiVersion >= 1) {
        writer->WriteInt32(ControllerId);
    }
    writer->WriteInt32(Topics.size());
    for (const auto& topic : Topics) {
        topic.Serialize(writer, apiVersion);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TReqFindCoordinator::Deserialize(IKafkaProtocolReader* reader, int /*apiVersion*/)
{
    Key = reader->ReadString();
}

void TRspFindCoordinator::Serialize(IKafkaProtocolWriter* writer, int /*apiVersion*/) const
{
    writer->WriteInt16(ErrorCode);
    writer->WriteInt32(NodeId);
    writer->WriteString(Host);
    writer->WriteInt32(Port);
}

////////////////////////////////////////////////////////////////////////////////

void TReqJoinGroupProtocol::Deserialize(IKafkaProtocolReader *reader, int /*apiVersion*/)
{
    Name = reader->ReadString();
    Metadata = reader->ReadBytes();
}

void TReqJoinGroup::Deserialize(IKafkaProtocolReader* reader, int apiVersion)
{
    GroupId = reader->ReadString();
    SessionTimeoutMs = reader->ReadInt32();
    MemberId = reader->ReadString();
    ProtocolType = reader->ReadString();
    Protocols.resize(reader->ReadInt32());
    for (auto& protocol : Protocols) {
        protocol.Deserialize(reader, apiVersion);
    }
}

void TRspJoinGroupMember::Serialize(IKafkaProtocolWriter* writer, int /*apiVersion*/) const
{
    writer->WriteString(MemberId);
    writer->WriteBytes(Metadata);
}

void TRspJoinGroup::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    writer->WriteInt16(ErrorCode);
    writer->WriteInt32(GenerationId);
    writer->WriteString(ProtocolName);
    writer->WriteString(Leader);
    writer->WriteString(MemberId);

    writer->WriteInt32(Members.size());
    for (const auto& member : Members) {
        member.Serialize(writer, apiVersion);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TReqSyncGroupAssignment::Deserialize(IKafkaProtocolReader* reader, int /*apiVersion*/)
{
    MemberId = reader->ReadString();
    Assignment = reader->ReadBytes();
}

void TReqSyncGroup::Deserialize(IKafkaProtocolReader* reader, int apiVersion)
{
    GroupId = reader->ReadString();
    GenerationId = reader->ReadString();
    MemberId = reader->ReadString();
    Assignments.resize(reader->ReadInt32());
    for (auto& assignment : Assignments) {
        assignment.Deserialize(reader, apiVersion);
    }
}

void TRspSyncGroupAssignment::Serialize(IKafkaProtocolWriter* writer, int /*apiVersion*/) const
{
    writer->WriteString(Topic);
    writer->WriteInt32(Partitions.size());
    for (const auto& partition : Partitions) {
        writer->WriteInt32(partition);
    }
}

void TRspSyncGroup::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    writer->WriteInt16(ErrorCode);

    writer->StartBytes();
    writer->WriteInt16(0);
    writer->WriteInt32(Assignments.size());
    for (const auto& assignment : Assignments) {
        assignment.Serialize(writer, apiVersion);
    }
    // User data.
    writer->WriteBytes(TString{});
    writer->FinishBytes();
}

////////////////////////////////////////////////////////////////////////////////

void TReqHeartbeat::Deserialize(IKafkaProtocolReader* reader, int /*apiVersion*/)
{
    GroupId = reader->ReadString();
    GenerationId = reader->ReadInt32();
    MemberId = reader->ReadString();
}

void TRspHeartbeat::Serialize(IKafkaProtocolWriter* writer, int /*apiVersion*/) const
{
    writer->WriteInt16(ErrorCode);
}

////////////////////////////////////////////////////////////////////////////////

void TReqOffsetFetchTopic::Deserialize(IKafkaProtocolReader* reader, int /*apiVersion*/)
{
    Name = reader->ReadString();
    PartitionIndexes.resize(reader->ReadInt32());
    for (auto& partitionIndex : PartitionIndexes) {
        partitionIndex = reader->ReadInt32();
    }
}

void TReqOffsetFetch::Deserialize(IKafkaProtocolReader* reader, int apiVersion)
{
    GroupId = reader->ReadString();
    Topics.resize(reader->ReadInt32());
    for (auto& topic : Topics) {
        topic.Deserialize(reader, apiVersion);
    }
}

void TRspOffsetFetchTopicPartition::Serialize(IKafkaProtocolWriter* writer, int /*apiVersion*/) const
{
    writer->WriteInt32(PartitionIndex);
    writer->WriteInt64(CommittedOffset);
    writer->WriteNullableString(Metadata);
    writer->WriteInt16(ErrorCode);
}

void TRspOffsetFetchTopic::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    writer->WriteString(Name);
    writer->WriteInt32(Partitions.size());
    for (const auto& partition : Partitions) {
        partition.Serialize(writer, apiVersion);
    }
}

void TRspOffsetFetch::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    writer->WriteInt32(Topics.size());
    for (const auto& topic : Topics) {
        topic.Serialize(writer, apiVersion);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TReqFetchTopicPartition::Deserialize(IKafkaProtocolReader* reader, int /*apiVersion*/)
{
    Partition = reader->ReadInt32();
    FetchOffset = reader->ReadInt64();
    PartitionMaxBytes = reader->ReadInt32();
}

void TReqFetchTopic::Deserialize(IKafkaProtocolReader* reader, int apiVersion)
{
    Topic = reader->ReadString();
    Partitions.resize(reader->ReadInt32());
    for (auto& partition : Partitions) {
        partition.Deserialize(reader, apiVersion);
    }
}

void TReqFetch::Deserialize(IKafkaProtocolReader* reader, int apiVersion)
{
    ReplicaId = reader->ReadInt32();
    MaxWaitMs = reader->ReadInt32();
    MinBytes = reader->ReadInt32();
    Topics.resize(reader->ReadInt32());
    for (auto& topic : Topics) {
        topic.Deserialize(reader, apiVersion);
    }
}

void TMessage::Serialize(IKafkaProtocolWriter* writer, int /*apiVersion*/) const
{
    writer->WriteInt32(Crc);
    writer->WriteByte(MagicByte);
    writer->WriteByte(Attributes);
    writer->WriteBytes(Key);
    writer->WriteBytes(Value);
}

void TRecord::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    writer->WriteInt64(Offset);
    writer->StartBytes();
    Message.Serialize(writer, apiVersion);
    writer->FinishBytes();
}

void TRspFetchResponsePartition::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    writer->WriteInt32(PartitionIndex);
    writer->WriteInt16(ErrorCode);
    writer->WriteInt64(HighWatermark);

    if (!Records) {
        writer->WriteInt32(-1);
    } else {
        writer->StartBytes();
        for (const auto& record : *Records) {
            record.Serialize(writer, apiVersion);
        }
        writer->FinishBytes();
    }
}

void TRspFetchResponse::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    writer->WriteString(Topic);
    writer->WriteInt32(Partitions.size());
    for (const auto& partition : Partitions) {
        partition.Serialize(writer, apiVersion);
    }
}

void TRspFetch::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    writer->WriteInt32(Responses.size());
    for (const auto& response : Responses) {
        response.Serialize(writer, apiVersion);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TReqSaslHandshake::Deserialize(IKafkaProtocolReader* reader, int /*apiVersion*/)
{
    Mechanism = reader->ReadString();
}

void TRspSaslHandshake::Serialize(IKafkaProtocolWriter* writer, int /*apiVersion*/) const
{
    writer->WriteErrorCode(ErrorCode);
    writer->WriteInt32(Mechanisms.size());
    for (const auto& mechanism : Mechanisms) {
        writer->WriteString(mechanism);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TReqSaslAuthenticate::Deserialize(IKafkaProtocolReader* reader, int /*apiVersion*/)
{
    AuthBytes = reader->ReadBytes();
}

void TRspSaslAuthenticate::Serialize(IKafkaProtocolWriter* writer, int /*apiVersion*/) const
{
    writer->WriteErrorCode(ErrorCode);
    writer->WriteNullableString(ErrorMessage);
    writer->WriteBytes(AuthBytes);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafka
