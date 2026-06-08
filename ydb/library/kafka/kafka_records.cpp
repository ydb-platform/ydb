#include "kafka_messages_int.h"

#include <library/cpp/streams/zstd/zstd.h>

#include <util/stream/mem.h>
#include <util/stream/str.h>
#include <util/stream/zlib.h>

namespace NKafka {

namespace {

ECompressionType GetCompressionType(TKafkaRecordBatch::AttributesMeta::Type attributes) {
    return static_cast<ECompressionType>(attributes & 0x07);
}

void EnsureSupportedCompressionType(ECompressionType compressionType) {
    switch (compressionType) {
        case ECompressionType::NONE:
        case ECompressionType::GZIP:
        case ECompressionType::ZSTD:
            return;
        default:
            ythrow yexception() << "unsupported Kafka record batch compression type: " << static_cast<int>(compressionType);
    }
}

void EnsureValidRecordBatchRecordsCount(TKafkaInt32 recordsCount) {
    if (recordsCount < 0) {
        ythrow yexception() << "non-nullable field records was serialized as null";
    }
}

TString DecompressRecordBatchPayload(TStringBuf data, ECompressionType compressionType) {
    EnsureSupportedCompressionType(compressionType);
    if (compressionType == ECompressionType::NONE) {
        return TString(data);
    }

    TMemoryInput input(data.data(), data.size());
    switch (compressionType) {
        case ECompressionType::GZIP: {
            TZLibDecompress gzip(&input, ZLib::GZip);
            return gzip.ReadAll();
        }
        case ECompressionType::ZSTD: {
            TZstdDecompress zstd(&input);
            return zstd.ReadAll();
        }
        default:
            ythrow yexception() << "unsupported Kafka record batch compression type: " << static_cast<int>(compressionType);
    }
}

TString CompressRecordBatchPayload(TStringBuf data, ECompressionType compressionType) {
    EnsureSupportedCompressionType(compressionType);
    if (compressionType == ECompressionType::NONE) {
        return TString(data);
    }

    TString result;
    TStringOutput output(result);
    switch (compressionType) {
        case ECompressionType::GZIP: {
            TZLibCompress gzip(&output, ZLib::GZip);
            gzip.Write(data.data(), data.size());
            gzip.Finish();
            output.Finish();
            return result;
        }
        case ECompressionType::ZSTD: {
            TZstdCompress zstd(&output);
            zstd.Write(data.data(), data.size());
            zstd.Finish();
            output.Finish();
            return result;
        }
        default:
            ythrow yexception() << "unsupported Kafka record batch compression type: " << static_cast<int>(compressionType);
    }
}

TString SerializeRecordBatchRecords(
    const TKafkaRecordBatch::RecordsMeta::Type& records,
    TKafkaVersion version,
    bool includeArraySize) {
    using RecordsMeta = TKafkaRecordBatch::RecordsMeta;
    using ItemStrategy = NPrivate::TypeStrategy<
        RecordsMeta,
        RecordsMeta::ItemType,
        RecordsMeta::ItemTypeDesc>;

    NPrivate::TSizeCollector collector;
    i64 payloadSize = NPrivate::TypeStrategy<
        RecordsMeta, RecordsMeta::Type, NPrivate::TKafkaArrayDesc>::DoSize(collector, version, records);
    if (!includeArraySize) {
        payloadSize -= NPrivate::ArraySize<RecordsMeta>(version, static_cast<TKafkaInt32>(records.size()));
    }

    TKafkaWriteBuffer buffer(payloadSize);
    TKafkaWritable writable(buffer);
    if (includeArraySize) {
        NPrivate::TWriteCollector writeCollector;
        NPrivate::Write<RecordsMeta>(writeCollector, writable, version, records);
    } else {
        for (const auto& record : records) {
            ItemStrategy::DoWrite(writable, version, record);
        }
    }

    TString result;
    for (auto it = buffer.GetBuffersDeque().rbegin(); it != buffer.GetBuffersDeque().rend(); ++it) {
        result.append(it->Data(), it->Size());
    }
    return result;
}

} // namespace

//
// TKafkaHeader
//
const TKafkaHeader::KeyMeta::Type TKafkaHeader::KeyMeta::Default = std::nullopt;

TKafkaHeader::TKafkaHeader()
    : Key(KeyMeta::Default) {
}

void TKafkaHeader::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TKafkaHeader";
    }
    NPrivate::Read<KeyMeta>(_readable, _version, Key);
    NPrivate::Read<ValueMeta>(_readable, _version, Value);
}

void TKafkaHeader::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TKafkaHeader";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<KeyMeta>(_collector, _writable, _version, Key);
    NPrivate::Write<ValueMeta>(_collector, _writable, _version, Value);
}

i32 TKafkaHeader::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<KeyMeta>(_collector, _version, Key);
    NPrivate::Size<ValueMeta>(_collector, _version, Value);

    return _collector.Size;
}


//
// TKafkaRecord
//
const TKafkaRecord::KeyMeta::Type TKafkaRecord::KeyMeta::Default = std::nullopt;

TKafkaRecord::TKafkaRecord()
    : Length(LengthMeta::Default)
    , Attributes(AttributesMeta::Default)
    , TimestampDelta(TimestampDeltaMeta::Default)
    , OffsetDelta(OffsetDeltaMeta::Default)
    , Key(KeyMeta::Default)
{
}

TKafkaRecord::TKafkaRecord(const TKafkaRecord& other)
    : Length(other.Length)
    , Attributes(other.Attributes)
    , TimestampDelta(other.TimestampDelta)
    , OffsetDelta(other.OffsetDelta)
    , Key(other.Key)
    , Value(other.Value)
    , Headers(other.Headers)
    , Storage_(other.Storage_)
{
    RebindStorage();
}

TKafkaRecord::TKafkaRecord(TKafkaRecord&& other) noexcept
    : Length(other.Length)
    , Attributes(other.Attributes)
    , TimestampDelta(other.TimestampDelta)
    , OffsetDelta(other.OffsetDelta)
    , Key(other.Key)
    , Value(other.Value)
    , Headers(std::move(other.Headers))
    , Storage_(std::move(other.Storage_))
{
    RebindStorage();
}

TKafkaRecord& TKafkaRecord::operator=(const TKafkaRecord& other) {
    if (this != &other) {
        Length = other.Length;
        Attributes = other.Attributes;
        TimestampDelta = other.TimestampDelta;
        OffsetDelta = other.OffsetDelta;
        Key = other.Key;
        Value = other.Value;
        Headers = other.Headers;
        Storage_ = other.Storage_;
        RebindStorage();
    }
    return *this;
}

TKafkaRecord& TKafkaRecord::operator=(TKafkaRecord&& other) noexcept {
    if (this != &other) {
        Length = other.Length;
        Attributes = other.Attributes;
        TimestampDelta = other.TimestampDelta;
        OffsetDelta = other.OffsetDelta;
        Key = other.Key;
        Value = other.Value;
        Headers = std::move(other.Headers);
        Storage_ = std::move(other.Storage_);
        RebindStorage();
    }
    return *this;
}

void TKafkaRecord::SetKey(TString key) {
    Storage_.Key = std::move(key);
    Key = TArrayRef<const char>(Storage_.Key->data(), Storage_.Key->size());
}

void TKafkaRecord::SetValue(TString value) {
    Storage_.Value = std::move(value);
    Value = TArrayRef<const char>(Storage_.Value->data(), Storage_.Value->size());
}

void TKafkaRecord::AddHeader(TString key, TString value) {
    Storage_.Headers.push_back(TStorage::THeaderData{
        .Key = std::move(key),
        .Value = std::move(value),
    });
    RebindStorage();
}

void TKafkaRecord::OwnPayload() {
    if (Key) {
        Storage_.Key = TString(Key->data(), Key->size());
    } else {
        Storage_.Key.reset();
    }

    if (Value) {
        Storage_.Value = TString(Value->data(), Value->size());
    } else {
        Storage_.Value.reset();
    }

    if (!Headers.empty()) {
        std::vector<TStorage::THeaderData> headersStorage;
        headersStorage.reserve(Headers.size());
        for (const auto& header : Headers) {
            TStorage::THeaderData headerStorage;
            if (header.Key) {
                headerStorage.Key = TString(header.Key->data(), header.Key->size());
            }
            if (header.Value) {
                headerStorage.Value = TString(header.Value->data(), header.Value->size());
            }
            headersStorage.push_back(std::move(headerStorage));
        }
        Storage_.Headers = std::move(headersStorage);
    } else {
        Storage_.Headers.clear();
    }

    RebindStorage();
}

void TKafkaRecord::RebindStorage() {
    if (Storage_.Key) {
        Key = TArrayRef<const char>(Storage_.Key->data(), Storage_.Key->size());
    }
    if (Storage_.Value) {
        Value = TArrayRef<const char>(Storage_.Value->data(), Storage_.Value->size());
    }
    if (!Storage_.Headers.empty()) {
        Headers.clear();
        Headers.reserve(Storage_.Headers.size());
        for (const auto& headerStorage : Storage_.Headers) {
            TKafkaHeader header;
            if (headerStorage.Key) {
                header.Key = TArrayRef<const char>(headerStorage.Key->data(), headerStorage.Key->size());
            }
            if (headerStorage.Value) {
                header.Value = TArrayRef<const char>(headerStorage.Value->data(), headerStorage.Value->size());
            }
            Headers.push_back(std::move(header));
        }
    }
}

bool TKafkaRecord::operator==(const TKafkaRecord& other) const {
    return Length == other.Length
        && Attributes == other.Attributes
        && TimestampDelta == other.TimestampDelta
        && OffsetDelta == other.OffsetDelta
        && Key == other.Key
        && Value == other.Value
        && Headers == other.Headers;
}

void TKafkaRecord::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TKafkaRecord";
    }
    NPrivate::Read<LengthMeta>(_readable, _version, Length);
    NPrivate::Read<AttributesMeta>(_readable, _version, Attributes);
    NPrivate::Read<TimestampDeltaMeta>(_readable, _version, TimestampDelta);
    NPrivate::Read<OffsetDeltaMeta>(_readable, _version, OffsetDelta);
    NPrivate::Read<KeyMeta>(_readable, _version, Key);
    NPrivate::Read<ValueMeta>(_readable, _version, Value);
    NPrivate::Read<HeadersMeta>(_readable, _version, Headers);
}

void TKafkaRecord::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TKafkaRecord";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<LengthMeta>(_collector, _writable, _version, Length);
    NPrivate::Write<AttributesMeta>(_collector, _writable, _version, Attributes);
    NPrivate::Write<TimestampDeltaMeta>(_collector, _writable, _version, TimestampDelta);
    NPrivate::Write<OffsetDeltaMeta>(_collector, _writable, _version, OffsetDelta);
    NPrivate::Write<KeyMeta>(_collector, _writable, _version, Key);
    NPrivate::Write<ValueMeta>(_collector, _writable, _version, Value);
    NPrivate::Write<HeadersMeta>(_collector, _writable, _version, Headers);
}

i32 TKafkaRecord::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<LengthMeta>(_collector, _version, Length);
    NPrivate::Size<AttributesMeta>(_collector, _version, Attributes);
    NPrivate::Size<TimestampDeltaMeta>(_collector, _version, TimestampDelta);
    NPrivate::Size<OffsetDeltaMeta>(_collector, _version, OffsetDelta);
    NPrivate::Size<KeyMeta>(_collector, _version, Key);
    NPrivate::Size<ValueMeta>(_collector, _version, Value);
    NPrivate::Size<HeadersMeta>(_collector, _version, Headers);
    
    return _collector.Size;
}



//
// TKafkaRecordBatch
//
TKafkaRecordBatch::TKafkaRecordBatch()
    : BaseOffset(BaseOffsetMeta::Default)
    , BatchLength(BatchLengthMeta::Default)
    , PartitionLeaderEpoch(PartitionLeaderEpochMeta::Default)
    , Magic(MagicMeta::Default)
    , Crc(CrcMeta::Default)
    , Attributes(AttributesMeta::Default)
    , LastOffsetDelta(LastOffsetDeltaMeta::Default)
    , BaseTimestamp(BaseTimestampMeta::Default)
    , MaxTimestamp(MaxTimestampMeta::Default)
    , ProducerId(ProducerIdMeta::Default)
    , ProducerEpoch(ProducerEpochMeta::Default)
    , BaseSequence(BaseSequenceMeta::Default)
    , RecordsCount(0) {
    }

ECompressionType TKafkaRecordBatch::CompressionType() const {
    return GetCompressionType(Attributes);
}

ETimestampType TKafkaRecordBatch::TimestampType() const {
    return (Attributes & 0x08) ? ETimestampType::LOG_APPEND_TIME : ETimestampType::CREATE_TIME;
}

bool TKafkaRecordBatch::Transactional() const {
    return Attributes & 0x10;
}

bool TKafkaRecordBatch::ControlBatch() const {
    return Attributes & 0x20;
}

bool TKafkaRecordBatch::HasDeleteHorizonMs() const {
    return Attributes & 0x40;
}

void TKafkaRecordBatch::Compress(TKafkaVersion version) {
    const auto compressionType = CompressionType();
    EnsureSupportedCompressionType(compressionType);
    if (compressionType == ECompressionType::NONE) {
        TString().swap(PackedRecords);
        return;
    }
    if (!PackedRecords.empty()) {
        return;
    }

    RecordsCount = static_cast<TKafkaInt32>(Records.size());
    PackedRecords = CompressRecordBatchPayload(
        SerializeRecordBatchRecords(Records, version, false), compressionType);
}

void TKafkaRecordBatch::EnsurePackedRecords(TKafkaVersion version) const {
    if (CompressionType() == ECompressionType::NONE || !PackedRecords.empty()) {
        return;
    }
    const_cast<TKafkaRecordBatch*>(this)->Compress(version);
}

void TKafkaRecordBatch::Decompress(TKafkaVersion version) {
    const auto compressionType = CompressionType();
    if (compressionType == ECompressionType::NONE || PackedRecords.empty()) {
        return;
    }

    EnsureSupportedCompressionType(compressionType);
    const TString decompressed = DecompressRecordBatchPayload(PackedRecords, compressionType);
    TBuffer buffer(decompressed.data(), decompressed.size());
    TKafkaReadable recordsReadable(buffer);
    Records.clear();
    EnsureValidRecordBatchRecordsCount(RecordsCount);
    Records.resize(RecordsCount);
    using ItemStrategy = NPrivate::TypeStrategy<
        RecordsMeta,
        RecordsMeta::ItemType,
        RecordsMeta::ItemTypeDesc>;
    for (auto& record : Records) {
        ItemStrategy::DoRead(recordsReadable, version, record);
    }
    for (auto& record : Records) {
        record.OwnPayload();
    }
    TString().swap(PackedRecords);
}

void TKafkaRecordBatch::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    const auto& compression = _readable.GetCompression();
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TKafkaRecordBatch";
    }
    NPrivate::Read<BaseOffsetMeta>(_readable, _version, BaseOffset);
    NPrivate::Read<BatchLengthMeta>(_readable, _version, BatchLength);
    NPrivate::Read<PartitionLeaderEpochMeta>(_readable, _version, PartitionLeaderEpoch);
    NPrivate::Read<MagicMeta>(_readable, _version, Magic);
    if (2 != Magic) {
        ythrow yexception() << "Supported only RecordBatch version 2 but " << (ui16)Magic;
    }

    NPrivate::Read<CrcMeta>(_readable, _version, Crc);
    NPrivate::Read<AttributesMeta>(_readable, _version, Attributes);
    if (CompressionType() != ECompressionType::NONE && !compression.AllowCompressed) {
        ythrow yexception() << "Supported only CompressionType::NONE";
    }
    EnsureSupportedCompressionType(CompressionType());

    NPrivate::Read<LastOffsetDeltaMeta>(_readable, _version, LastOffsetDelta);
    NPrivate::Read<BaseTimestampMeta>(_readable, _version, BaseTimestamp);
    NPrivate::Read<MaxTimestampMeta>(_readable, _version, MaxTimestamp);
    NPrivate::Read<ProducerIdMeta>(_readable, _version, ProducerId);
    NPrivate::Read<ProducerEpochMeta>(_readable, _version, ProducerEpoch);
    NPrivate::Read<BaseSequenceMeta>(_readable, _version, BaseSequence);
    TString().swap(PackedRecords);
    Records.clear();

    if (CompressionType() == ECompressionType::NONE) {
        NPrivate::Read<RecordsMeta>(_readable, _version, Records);
        RecordsCount = static_cast<TKafkaInt32>(Records.size());
    } else if (compression.SkipDecompression) {
        RecordsCount = NPrivate::ReadArraySize<RecordsMeta>(_readable, _version);
        EnsureValidRecordBatchRecordsCount(RecordsCount);
        const auto compressed = _readable.Bytes(_readable.left());
        PackedRecords.assign(compressed.data(), compressed.size());
    } else {
        RecordsCount = NPrivate::ReadArraySize<RecordsMeta>(_readable, _version);
        EnsureValidRecordBatchRecordsCount(RecordsCount);
        const auto compressed = _readable.Bytes(_readable.left());
        PackedRecords.assign(compressed.data(), compressed.size());
        Decompress(_version);
    }
}

void TKafkaRecordBatch::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TKafkaRecordBatch";
    }
    EnsureSupportedCompressionType(CompressionType());
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<BaseOffsetMeta>(_collector, _writable, _version, BaseOffset);
    NPrivate::Write<BatchLengthMeta>(_collector, _writable, _version, BatchLength);
    NPrivate::Write<PartitionLeaderEpochMeta>(_collector, _writable, _version, PartitionLeaderEpoch);
    NPrivate::Write<MagicMeta>(_collector, _writable, _version, Magic);
    NPrivate::Write<CrcMeta>(_collector, _writable, _version, Crc);
    NPrivate::Write<AttributesMeta>(_collector, _writable, _version, Attributes);
    NPrivate::Write<LastOffsetDeltaMeta>(_collector, _writable, _version, LastOffsetDelta);
    NPrivate::Write<BaseTimestampMeta>(_collector, _writable, _version, BaseTimestamp);
    NPrivate::Write<MaxTimestampMeta>(_collector, _writable, _version, MaxTimestamp);
    NPrivate::Write<ProducerIdMeta>(_collector, _writable, _version, ProducerId);
    NPrivate::Write<ProducerEpochMeta>(_collector, _writable, _version, ProducerEpoch);
    NPrivate::Write<BaseSequenceMeta>(_collector, _writable, _version, BaseSequence);
    if (CompressionType() == ECompressionType::NONE) {
        NPrivate::Write<RecordsMeta>(_collector, _writable, _version, Records);
    } else {
        EnsurePackedRecords(_version);
        NPrivate::WriteArraySize<RecordsMeta>(_writable, _version, RecordsCount);
        _writable.write(PackedRecords.data(), PackedRecords.size());
    }
}

i32 TKafkaRecordBatch::Size(TKafkaVersion _version) const {
    EnsureSupportedCompressionType(CompressionType());
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<BaseOffsetMeta>(_collector, _version, BaseOffset);
    NPrivate::Size<BatchLengthMeta>(_collector, _version, BatchLength);
    NPrivate::Size<PartitionLeaderEpochMeta>(_collector, _version, PartitionLeaderEpoch);
    NPrivate::Size<MagicMeta>(_collector, _version, Magic);
    NPrivate::Size<CrcMeta>(_collector, _version, Crc);
    NPrivate::Size<AttributesMeta>(_collector, _version, Attributes);
    NPrivate::Size<LastOffsetDeltaMeta>(_collector, _version, LastOffsetDelta);
    NPrivate::Size<BaseTimestampMeta>(_collector, _version, BaseTimestamp);
    NPrivate::Size<MaxTimestampMeta>(_collector, _version, MaxTimestamp);
    NPrivate::Size<ProducerIdMeta>(_collector, _version, ProducerId);
    NPrivate::Size<ProducerEpochMeta>(_collector, _version, ProducerEpoch);
    NPrivate::Size<BaseSequenceMeta>(_collector, _version, BaseSequence);
    if (CompressionType() == ECompressionType::NONE) {
        NPrivate::Size<RecordsMeta>(_collector, _version, Records);
    } else {
        EnsurePackedRecords(_version);
        _collector.Size += NPrivate::ArraySize<RecordsMeta>(_version, RecordsCount) + PackedRecords.size();
    }

    return _collector.Size;
}



//
// TKafkaRecordV0
//
const TKafkaRecordV0::KeyMeta::Type TKafkaRecordV0::KeyMeta::Default = std::nullopt;

TKafkaRecordV0::TKafkaRecordV0()
    : MessageSize(MessageSizeMeta::Default)
    , Crc(CrcMeta::Default)
    , Magic(MagicMeta::Default)
    , Attributes(AttributesMeta::Default)
    , Timestamp(TimestampMeta::Default)
    , Key(KeyMeta::Default) {
}

void TKafkaRecordV0::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TKafkaRecordV0";
    }
    NPrivate::Read<MessageSizeMeta>(_readable, _version, MessageSize);
    NPrivate::Read<CrcMeta>(_readable, _version, Crc);
    NPrivate::Read<MagicMeta>(_readable, _version, Magic);
    NPrivate::Read<AttributesMeta>(_readable, _version, Attributes);
    NPrivate::Read<TimestampMeta>(_readable, _version, Timestamp);
    NPrivate::Read<KeyMeta>(_readable, _version, Key);
    NPrivate::Read<ValueMeta>(_readable, _version, Value);
}

void TKafkaRecordV0::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TKafkaRecordV0";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<MessageSizeMeta>(_collector, _writable, _version, MessageSize);
    NPrivate::Write<CrcMeta>(_collector, _writable, _version, Crc);
    NPrivate::Write<MagicMeta>(_collector, _writable, _version, Magic);
    NPrivate::Write<AttributesMeta>(_collector, _writable, _version, Attributes);
    NPrivate::Write<TimestampMeta>(_collector, _writable, _version, Timestamp);
    NPrivate::Write<KeyMeta>(_collector, _writable, _version, Key);
    NPrivate::Write<ValueMeta>(_collector, _writable, _version, Value);
}

i32 TKafkaRecordV0::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<MessageSizeMeta>(_collector, _version, MessageSize);
    NPrivate::Size<CrcMeta>(_collector, _version, Crc);
    NPrivate::Size<MagicMeta>(_collector, _version, Magic);
    NPrivate::Size<AttributesMeta>(_collector, _version, Attributes);
    NPrivate::Size<TimestampMeta>(_collector,  _version, Timestamp);
    NPrivate::Size<KeyMeta>(_collector, _version, Key);
    NPrivate::Size<ValueMeta>(_collector, _version, Value);

    return _collector.Size;
}



//
// TKafkaRecordV0
//
TKafkaRecordBatchV0::TKafkaRecordBatchV0()
    : Offset(OffsetMeta::Default) {
}

void TKafkaRecordBatchV0::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TKafkaRecordBatchV0";
    }
    NPrivate::Read<OffsetMeta>(_readable, _version, Offset);
    NPrivate::Read<RecordMeta>(_readable, _version, Record);
}

void TKafkaRecordBatchV0::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TKafkaRecordBatchV0";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<OffsetMeta>(_collector, _writable, _version, Offset);
    NPrivate::Write<RecordMeta>(_collector, _writable, _version, Record);
}

i32 TKafkaRecordBatchV0::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<OffsetMeta>(_collector, _version, Offset);
    NPrivate::Size<RecordMeta>(_collector, _version, Record);

    return _collector.Size;
}

TKafkaRecordBatch ReadKafkaRecordBatch(TStringBuf data, TKafkaVersion version, TKafkaCompression compression) {
    TBuffer buffer(data.data(), data.size());
    TKafkaReadable readable(buffer);
    readable.SetCompression(compression);

    TKafkaRecordBatch batch;
    batch.Read(readable, version);
    if (readable.left() != 0) {
        ythrow yexception() << "unexpected extra bytes after Kafka record batch: " << readable.left();
    }
    return batch;   
}

TString WriteKafkaRecordBatch(const TKafkaRecordBatch& batch, TKafkaVersion version) {
    TKafkaWriteBuffer buffer(batch.Size(version));
    TKafkaWritable writable(buffer);
    batch.Write(writable, version);
    return buffer.AsString();
}
} // namespace NKafka
