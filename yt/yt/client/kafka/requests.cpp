#include "requests.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NKafka {

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = KafkaLogger;

////////////////////////////////////////////////////////////////////////////////

int GetRequestHeaderVersion(ERequestType requestType, i16 apiVersion)
{
    switch (requestType) {
        case ERequestType::ApiVersions: {
            if (apiVersion >= 3) {
                return 2;
            }
            return 1;
        }
        case ERequestType::Metadata: {
            if (apiVersion >= 9) {
                return 2;
            }
            return 1;
        }
        case ERequestType::Fetch: {
            // TODO(nadya73): add version check
            return 1;
        }
        case ERequestType::Produce: {
            if (apiVersion >= 9) {
                return 2;
            }
            return 1;
        }
        case ERequestType::SaslHandshake: {
            return 1;
        }
        default: {
            return 1;
        }
    }
}

int GetResponseHeaderVersion(ERequestType requestType, i16 apiVersion)
{
    if (requestType == ERequestType::ApiVersions) {
        return 0;
    }
    return GetRequestHeaderVersion(requestType, apiVersion) - 1;
}

void TRequestHeader::Deserialize(IKafkaProtocolReader* reader)
{
    auto apiKey = reader->ReadInt16();
    RequestType = static_cast<ERequestType>(apiKey);
    ApiVersion = reader->ReadInt16();
    CorrelationId = reader->ReadInt32();

    auto version = GetRequestHeaderVersion(RequestType, ApiVersion);

    if (version >= 1) {
        ClientId = reader->ReadNullableString();
    }

    if (version >= 2) {
        NKafka::Deserialize(TagBuffer, reader, /*isCompact*/ true);
    }
}

void TResponseHeader::Serialize(IKafkaProtocolWriter* writer, int version)
{
    writer->WriteInt32(CorrelationId);

    if (version >= 1) {
        NKafka::Serialize(TagBuffer, writer, /*isCompact*/ true);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TRecordHeader::Serialize(IKafkaProtocolWriter* writer, int /*version*/) const
{
    writer->WriteVarInt(HeaderKey.size());
    writer->WriteData(HeaderKey);

    writer->WriteVarInt(HeaderValue.size());
    writer->WriteData(HeaderValue);
}

void TRecordHeader::Deserialize(IKafkaProtocolReader* reader, int /*version*/)
{
    auto keySize = reader->ReadVarInt();
    reader->ReadString(&HeaderKey, keySize);

    auto valueSize = reader->ReadVarInt();
    reader->ReadString(&HeaderValue, valueSize);
}

void TRecord::Serialize(IKafkaProtocolWriter* writer, int version) const
{
    if (version == 2) {
        auto recordWriter = CreateKafkaProtocolWriter();

        WRITE_KAFKA_FIELD(recordWriter, WriteByte, Attributes)
        WRITE_KAFKA_FIELD(recordWriter, WriteVarLong, TimestampDelta)
        WRITE_KAFKA_FIELD(recordWriter, WriteVarInt, OffsetDelta)

        WRITE_KAFKA_FIELD(recordWriter, WriteVarInt, Key.size())
        WRITE_KAFKA_FIELD(recordWriter, WriteData, Key)

        WRITE_KAFKA_FIELD(recordWriter, WriteVarInt, Value.size())
        WRITE_KAFKA_FIELD(recordWriter, WriteData, Value)

        WRITE_KAFKA_FIELD(recordWriter, WriteVarInt, Headers.size())
        for (const auto& header : Headers) {
            header.Serialize(recordWriter.get(), version);
        }

        auto record = recordWriter->Finish();

        WRITE_KAFKA_FIELD(writer, WriteVarInt, record.size())
        writer->WriteData(record);
    } else if (version == 1 || version == 0) {
        writer->WriteByte(Attributes);

        if (version == 1) {
            writer->WriteInt64(TimestampDelta);
        }

        writer->WriteBytes(Key);
        writer->WriteBytes(Value);
    } else {
        THROW_ERROR_EXCEPTION("Unsupported Record version %v in serialization", version);
    }
}

void TRecord::Deserialize(IKafkaProtocolReader* reader, int version)
{
    std::optional<i32> length;
    if (version == 2) {
        READ_KAFKA_FIELD(length, ReadVarInt);
        reader->StartReadBytes(/*needReadSize*/ false);
    }
    READ_KAFKA_FIELD(Attributes, ReadByte)

    if (version == 2) {
        READ_KAFKA_FIELD(TimestampDelta, ReadVarLong)
        READ_KAFKA_FIELD(OffsetDelta, ReadVarInt)

        auto keySize = reader->ReadVarInt();
        YT_LOG_TRACE("Parsing Record (KeySize: %v)", keySize);
        reader->ReadString(&Key, keySize);

        i32 valueSize;
        READ_KAFKA_FIELD(valueSize, ReadVarInt);

        if (valueSize > 0) {
            YT_LOG_TRACE("Parsing Record (ValueSize: %v)", valueSize);
            reader->ReadString(&Value, valueSize);
        }

        i32 headerCount;
        READ_KAFKA_FIELD(headerCount, ReadVarInt);
        if (headerCount > 0) {
            Headers.resize(headerCount);
            for (auto& header : Headers) {
                header.Deserialize(reader, version);
            }
        }

        reader->FinishReadBytes();

        if (length && reader->GetReadBytesCount() != length) {
            YT_LOG_ERROR("Not all record bytes were read (Expected: %v, Actual: %v)", *length, reader->GetReadBytesCount());
        }
    } else if (version == 1 || version == 0) {
        if (version == 1) {
            READ_KAFKA_FIELD(TimestampDelta, ReadInt64)
        }
        Key = reader->ReadBytes();
        Value = reader->ReadBytes();
    } else {
        THROW_ERROR_EXCEPTION("Unsupported Record version %v in deserialization", version);
    }
}

void TRecordBatch::Serialize(IKafkaProtocolWriter* writer) const
{
    WRITE_KAFKA_FIELD(writer, WriteInt64, BaseOffset)

    writer->StartBytes();  // Write Length.

    if (MagicByte == 0 || MagicByte == 1) {
        // TODO(nadya73): implement it via [Start/Finish]CalculateChecksum and crc32.
        WRITE_KAFKA_FIELD(writer, WriteUint32, CrcOld)
        WRITE_KAFKA_FIELD(writer, WriteByte, MagicByte)

        YT_VERIFY(Records.size() == 1);
        Records[0].Serialize(writer, MagicByte);
    } else if (MagicByte == 2) {
        WRITE_KAFKA_FIELD(writer, WriteInt32, PartitionLeaderEpoch)
        WRITE_KAFKA_FIELD(writer, WriteByte, MagicByte)
        writer->StartCalculateChecksum();
        WRITE_KAFKA_FIELD(writer, WriteInt16, Attributes)
        WRITE_KAFKA_FIELD(writer, WriteInt32, LastOffsetDelta)
        WRITE_KAFKA_FIELD(writer, WriteInt64, FirstTimestamp)
        WRITE_KAFKA_FIELD(writer, WriteInt64, MaxTimestamp)
        WRITE_KAFKA_FIELD(writer, WriteInt64, ProducerId)
        WRITE_KAFKA_FIELD(writer, WriteInt16, ProducerEpoch)
        WRITE_KAFKA_FIELD(writer, WriteInt32, BaseSequence)

        WRITE_KAFKA_FIELD(writer, WriteInt32, Records.size())
        for (const auto& record : Records) {
            record.Serialize(writer, MagicByte);
        }
        writer->FinishCalculateChecksum();
    } else {
        THROW_ERROR_EXCEPTION("Unsupported MagicByte %v in RecordBatch serialization", static_cast<int>(MagicByte));
    }
    writer->FinishBytes();
}

void TRecordBatch::Deserialize(IKafkaProtocolReader* reader)
{
    READ_KAFKA_FIELD(BaseOffset, ReadInt64)
    READ_KAFKA_FIELD(Length, ReadInt32)

    reader->StartReadBytes(/*needReadSize*/ false);

    READ_KAFKA_FIELD(PartitionLeaderEpoch, ReadInt32)
    READ_KAFKA_FIELD(MagicByte, ReadByte)

    if (MagicByte == 0 || MagicByte == 1) {
        // In v0/v1 CRC is before MagicByte and there is no PartitionLeaderEpoch;
        CrcOld = PartitionLeaderEpoch;
        PartitionLeaderEpoch = 0;

        // It's a message in v0/v1.
        auto& record = Records.emplace_back();
        YT_LOG_TRACE("Parsing RecordBatch, reading Record");
        record.Deserialize(reader, MagicByte);
    } else if (MagicByte == 2) {
        READ_KAFKA_FIELD(Crc, ReadUint32)
        READ_KAFKA_FIELD(Attributes, ReadInt16)
        READ_KAFKA_FIELD(LastOffsetDelta, ReadInt32)
        READ_KAFKA_FIELD(FirstTimestamp, ReadInt64)
        READ_KAFKA_FIELD(MaxTimestamp, ReadInt64)
        READ_KAFKA_FIELD(ProducerId, ReadInt64)
        READ_KAFKA_FIELD(ProducerEpoch, ReadInt16)
        READ_KAFKA_FIELD(BaseSequence, ReadInt32)

        i32 recordCount = 0;
        READ_KAFKA_FIELD(recordCount, ReadInt32)
        if (recordCount > 0) {
            Records.reserve(recordCount);
            for (i32 recordIndex = 0; recordIndex < recordCount; ++recordIndex) {
                TRecord record;
                record.Deserialize(reader, MagicByte);
                Records.push_back(std::move(record));
            }
        }
        if (reader->GetReadBytesCount() != Length) {
            THROW_ERROR_EXCEPTION("Unexpected record batch length (Expected: %v, Actual: %v)", Length, reader->GetReadBytesCount());
        }
    } else {
        THROW_ERROR_EXCEPTION("Unsupported MagicByte %v in RecordBatch deserialization", static_cast<int>(MagicByte));
    }
    reader->FinishReadBytes();
}

////////////////////////////////////////////////////////////////////////////////

void TTaggedField::Serialize(IKafkaProtocolWriter* writer) const
{
    writer->WriteUnsignedVarInt(Tag);
    writer->WriteCompactBytes(Data);
}

void TTaggedField::Deserialize(IKafkaProtocolReader* reader)
{
    Tag = reader->ReadUnsignedVarInt();
    Data = reader->ReadCompactBytes();
}

////////////////////////////////////////////////////////////////////////////////

void TReqApiVersions::Deserialize(IKafkaProtocolReader* reader, int apiVersion)
{
    if (apiVersion <= 2) {
        return;
    }

    ClientSoftwareName = reader->ReadCompactString();
    ClientSoftwareVersion = reader->ReadCompactString();

    NKafka::Deserialize(TagBuffer, reader, /*isCompact*/ true);
}

void TRspApiKey::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    writer->WriteInt16(ApiKey);
    writer->WriteInt16(MinVersion);
    writer->WriteInt16(MaxVersion);

    if (apiVersion >= 3) {
        NKafka::Serialize(TagBuffer, writer, /*isCompact*/ true);
    }
}

void TRspApiVersions::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    writer->WriteErrorCode(ErrorCode);
    NKafka::Serialize(ApiKeys, writer, apiVersion >= 3, apiVersion);

    if (apiVersion >= 1) {
        writer->WriteInt32(ThrottleTimeMs);
    }
    if (apiVersion >= 3) {
        NKafka::Serialize(TagBuffer, writer, /*isCompact*/ true);
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
        Topic = reader->ReadCompactString();
    }
    if (apiVersion >= 9) {
        NKafka::Deserialize(TagBuffer, reader, /*isCompact*/ true);
    }
}

void TReqMetadata::Deserialize(IKafkaProtocolReader* reader, int apiVersion)
{
    NKafka::Deserialize(Topics, reader, apiVersion >= 9, apiVersion);

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
        NKafka::Deserialize(TagBuffer, reader, /*isCompact*/ true);
    }
}

void TRspMetadataBroker::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    writer->WriteInt32(NodeId);
    writer->WriteString(Host);
    writer->WriteInt32(Port);
    if (apiVersion >= 1) {
        writer->WriteNullableString(Rack);
    }
    if (apiVersion >= 9) {
        NKafka::Serialize(TagBuffer, writer, /*isCompact*/ true);
    }
}

void TRspMetadataTopicPartition::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    writer->WriteErrorCode(ErrorCode);
    writer->WriteInt32(PartitionIndex);
    writer->WriteInt32(LeaderId);
    // TODO(nadya73): check version.
    writer->WriteInt32(ReplicaNodes.size());
    for (auto replicaNode : ReplicaNodes) {
        writer->WriteInt32(replicaNode);
    }
     // TODO(nadya73): check version.
    writer->WriteInt32(IsrNodes.size());
    for (auto isrNode : IsrNodes) {
        writer->WriteInt32(isrNode);
    }
    if (apiVersion >= 9) {
        NKafka::Serialize(TagBuffer, writer, /*isCompact*/ true);
    }
}

void TRspMetadataTopic::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    writer->WriteErrorCode(ErrorCode);
    writer->WriteString(Name);
    if (apiVersion >= 1) {
        writer->WriteBool(IsInternal);
    }
    NKafka::Serialize(Partitions, writer, apiVersion >= 9, apiVersion);
    if (apiVersion >= 9) {
        NKafka::Serialize(TagBuffer, writer, /*isCompact*/ true);
    }
}

void TRspMetadata::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    NKafka::Serialize(Brokers, writer, apiVersion >= 9, apiVersion);
    if (apiVersion >= 2) {
        writer->WriteNullableString(ClusterId);
    }
    if (apiVersion >= 1) {
        writer->WriteInt32(ControllerId);
    }
    NKafka::Serialize(Topics, writer, apiVersion >= 9, apiVersion);
    if (apiVersion >= 9) {
        NKafka::Serialize(TagBuffer, writer, /*isCompact*/ true);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TReqFindCoordinator::Deserialize(IKafkaProtocolReader* reader, int /*apiVersion*/)
{
    Key = reader->ReadString();
}

void TRspFindCoordinator::Serialize(IKafkaProtocolWriter* writer, int /*apiVersion*/) const
{
    writer->WriteErrorCode(ErrorCode);
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

    NKafka::Deserialize(Protocols, reader, /*isCompact*/ false, apiVersion);
}

void TRspJoinGroupMember::Serialize(IKafkaProtocolWriter* writer, int /*apiVersion*/) const
{
    writer->WriteString(MemberId);
    writer->WriteBytes(Metadata);
}

void TRspJoinGroup::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    writer->WriteErrorCode(ErrorCode);
    writer->WriteInt32(GenerationId);
    writer->WriteString(ProtocolName);
    writer->WriteString(Leader);
    writer->WriteString(MemberId);

    NKafka::Serialize(Members, writer, /*isCompact*/ false, apiVersion);
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
    GenerationId = reader->ReadInt32();
    MemberId = reader->ReadString();

    NKafka::Deserialize(Assignments, reader, /*isCompact*/ false, apiVersion);
}

void TRspSyncGroup::Serialize(IKafkaProtocolWriter* writer, int /*apiVersion*/) const
{
    writer->WriteErrorCode(ErrorCode);
    writer->WriteBytes(Assignment);
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
    writer->WriteErrorCode(ErrorCode);
}

////////////////////////////////////////////////////////////////////////////////

void TReqLeaveGroup::Deserialize(IKafkaProtocolReader* reader, int /*apiVersion*/)
{
    GroupId = reader->ReadString();
    MemberId = reader->ReadString();
}

void TRspLeaveGroup::Serialize(IKafkaProtocolWriter* writer, int /*apiVersion*/) const
{
    writer->WriteErrorCode(ErrorCode);
}

////////////////////////////////////////////////////////////////////////////////

void TReqOffsetCommitTopicPartition::Deserialize(IKafkaProtocolReader* reader, int /*apiVersion*/)
{
    PartitionIndex = reader->ReadInt32();
    CommittedOffset = reader->ReadInt64();
    CommittedMetadata = reader->ReadNullableString();
}

void TReqOffsetCommitTopic::Deserialize(IKafkaProtocolReader* reader, int apiVersion)
{
    Name = reader->ReadString();
    NKafka::Deserialize(Partitions, reader, /*isCompact*/ apiVersion >= 8, apiVersion);
}

void TReqOffsetCommit::Deserialize(IKafkaProtocolReader* reader, int apiVersion)
{
    GroupId = reader->ReadString();
    NKafka::Deserialize(Topics, reader, /*isCompact*/ apiVersion >= 8, apiVersion);
}

void TRspOffsetCommitTopicPartition::Serialize(IKafkaProtocolWriter* writer, int /*apiVersion*/) const
{
    writer->WriteInt32(PartitionIndex);
    writer->WriteErrorCode(ErrorCode);
}

void TRspOffsetCommitTopic::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    writer->WriteString(Name);
    NKafka::Serialize(Partitions, writer, /*isCompact*/ apiVersion >= 8, apiVersion);
}

void TRspOffsetCommit::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    NKafka::Serialize(Topics, writer, /*isCompact*/ apiVersion >= 8, apiVersion);
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
    writer->WriteErrorCode(ErrorCode);
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
    ApiVersion = apiVersion;

    ReplicaId = reader->ReadInt32();
    MaxWaitMs = reader->ReadInt32();
    MinBytes = reader->ReadInt32();
    if (apiVersion >= 3) {
        MaxBytes = reader->ReadInt32();
    }
    Topics.resize(reader->ReadInt32());
    for (auto& topic : Topics) {
        topic.Deserialize(reader, apiVersion);
    }
}

void TRspFetchResponsePartition::Serialize(IKafkaProtocolWriter* writer, int /*apiVersion*/) const
{
    WRITE_KAFKA_FIELD(writer, WriteInt32, PartitionIndex)
    WRITE_KAFKA_FIELD(writer, WriteErrorCode, ErrorCode)
    WRITE_KAFKA_FIELD(writer, WriteInt64, HighWatermark)

    if (!RecordBatches) {
        i32 recordBatchesSize = -1;
        WRITE_KAFKA_FIELD(writer, WriteInt32, recordBatchesSize)
    } else {
        writer->StartBytes();
        for (const auto& recordBatch : *RecordBatches) {
            recordBatch.Serialize(writer);
        }
        writer->FinishBytes();
    }
}

void TRspFetchResponse::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    WRITE_KAFKA_FIELD(writer, WriteString, Topic)

    WRITE_KAFKA_FIELD(writer, WriteInt32, Partitions.size())
    for (const auto& partition : Partitions) {
        partition.Serialize(writer, apiVersion);
    }
}

void TRspFetch::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    if (apiVersion >= 2) {
        WRITE_KAFKA_FIELD(writer, WriteInt32, ThrottleTimeMs)
    }
    WRITE_KAFKA_FIELD(writer, WriteInt32, Responses.size())

    for (const auto& response : Responses) {
        response.Serialize(writer, apiVersion);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TReqSaslHandshake::Deserialize(IKafkaProtocolReader* reader, int apiVersion)
{
    ApiVersion = apiVersion;

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

void TReqProduceTopicDataPartitionData::Deserialize(IKafkaProtocolReader* reader, int apiVersion)
{
    READ_KAFKA_FIELD(Index, ReadInt32)

    i32 bytesCount;
    if (apiVersion < 9) {
        bytesCount = reader->StartReadBytes();
    } else {
        bytesCount = reader->StartReadCompactBytes();
    }
    while (reader->GetReadBytesCount() < bytesCount) {
        TRecordBatch recordBatch;
        recordBatch.Deserialize(reader);

        RecordBatches.push_back(std::move(recordBatch));
    }
    reader->FinishReadBytes();

    if (apiVersion >= 9) {
        NKafka::Deserialize(TagBuffer, reader, /*isCompact*/ true);
    }
}

void TReqProduceTopicData::Deserialize(IKafkaProtocolReader* reader, int apiVersion)
{
    if (apiVersion < 9) {
        READ_KAFKA_FIELD(Name, ReadString)
    } else {
        READ_KAFKA_FIELD(Name, ReadCompactString)
    }

    NKafka::Deserialize(PartitionData, reader, /*isCompact*/ apiVersion >= 9, apiVersion);

    if (apiVersion >= 9) {
        NKafka::Deserialize(TagBuffer, reader, /*isCompact*/ true);
    }
}

void TReqProduce::Deserialize(IKafkaProtocolReader* reader, int apiVersion)
{
    if (apiVersion >= 3) {
        if (apiVersion < 9) {
            READ_KAFKA_FIELD(TransactionalId, ReadNullableString)
        } else {
            READ_KAFKA_FIELD(TransactionalId, ReadCompactNullableString)
        }
    }
    READ_KAFKA_FIELD(Acks, ReadInt16)
    READ_KAFKA_FIELD(TimeoutMs, ReadInt32)

    NKafka::Deserialize(TopicData, reader, /*isCompact*/ apiVersion >= 9, apiVersion);

    if (apiVersion >= 9) {
        NKafka::Deserialize(TagBuffer, reader, /*isCompact*/ true);
    }
}

void TRspProduceResponsePartitionResponseRecordError::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    writer->WriteInt32(BatchIndex);
    if (apiVersion < 9) {
        writer->WriteNullableString(BatchIndexErrorMessage);
    } else {
        writer->WriteCompactNullableString(BatchIndexErrorMessage);
    }
    if (apiVersion >= 9) {
        NKafka::Serialize(TagBuffer, writer, /*isCompact*/ true);
    }
}

void TRspProduceResponsePartitionResponse::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    writer->WriteInt32(Index);
    writer->WriteErrorCode(ErrorCode);
    writer->WriteInt64(BaseOffset);
    if (apiVersion >= 2) {
        writer->WriteInt64(LogAppendTimeMs);
    }
    if (apiVersion >= 5) {
        writer->WriteInt64(LogStartOffset);
    }
    if (apiVersion >= 8) {
        NKafka::Serialize(RecordErrors, writer, apiVersion >= 9, apiVersion);

        if (apiVersion < 9) {
            writer->WriteNullableString(ErrorMessage);
        } else {
            writer->WriteCompactNullableString(ErrorMessage);
        }
    }
    if (apiVersion >= 9) {
        NKafka::Serialize(TagBuffer, writer, /*isCompact*/ true);
    }
}

void TRspProduceResponse::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    if (apiVersion < 9) {
        writer->WriteString(Name);
    } else {
        writer->WriteCompactString(Name);
    }
    NKafka::Serialize(PartitionResponses, writer, apiVersion >= 9, apiVersion);
    if (apiVersion >= 9) {
        NKafka::Serialize(TagBuffer, writer, /*isCompact*/ true);
    }
}

void TRspProduce::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    NKafka::Serialize(Responses, writer, apiVersion >= 9, apiVersion);
    if (apiVersion >= 1) {
        writer->WriteInt32(ThrottleTimeMs);
    }
    if (apiVersion >= 9) {
        NKafka::Serialize(TagBuffer, writer, /*isCompact*/ true);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TReqListOffsetsTopicPartition::Deserialize(IKafkaProtocolReader* reader, int /*apiVersion*/)
{
    PartitionIndex = reader->ReadInt32();
    Timestamp = reader->ReadInt64(); // TODO: use timestamp?
    MaxNumOffsets = reader->ReadInt32();
}

void TReqListOffsetsTopic::Deserialize(IKafkaProtocolReader* reader, int apiVersion)
{
    Name = reader->ReadString();
    Partitions.resize(reader->ReadInt32());
    for (auto& partition : Partitions) {
        partition.Deserialize(reader, apiVersion);
    }
}

void TReqListOffsets::Deserialize(IKafkaProtocolReader* reader, int apiVersion)
{
    ReplicaId = reader->ReadInt32();
    Topics.resize(reader->ReadInt32());
    for (auto& topic : Topics) {
        topic.Deserialize(reader, apiVersion);
    }
}

void TRspListOffsetsTopicPartition::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    writer->WriteInt32(PartitionIndex);
    writer->WriteErrorCode(ErrorCode);

    if (apiVersion <= 0) {
        writer->WriteInt32(1); // Size of 'old_style_offsets'.
    }
    writer->WriteInt64(Offset);
}

void TRspListOffsetsTopic::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    writer->WriteString(Name);
    writer->WriteInt32(Partitions.size());
    for (const auto& partition : Partitions) {
        partition.Serialize(writer, apiVersion);
    }
}

void TRspListOffsets::Serialize(IKafkaProtocolWriter* writer, int apiVersion) const
{
    writer->WriteInt32(Topics.size());
    for (const auto& topic : Topics) {
        topic.Serialize(writer, apiVersion);
    }
}

////////////////////////////////////////////////////////////////////////////////



} // namespace NYT::NKafka
