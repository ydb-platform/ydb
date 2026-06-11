#include "kafka_messages_int.h"

#include <library/cpp/streams/zstd/zstd.h>

#include <util/stream/mem.h>
#include <util/stream/str.h>
#include <util/stream/zlib.h>

namespace NKafka {

namespace {

static constexpr size_t WriteBufferChunkSize = 1 << 16;

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

    TKafkaWriteBuffer buffer(WriteBufferChunkSize);
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

// The returned entries keep TKafkaBytes views into `buffer`, so the caller must
// keep `buffer` alive for as long as the entries are used.
std::vector<TKafkaRecordBatchV0> ReadLegacyRecordEntries(const TBuffer& buffer, TKafkaVersion magic) {
    TKafkaReadable readable(buffer);
    std::vector<TKafkaRecordBatchV0> entries;

    while (readable.left() > 0) {
        const TKafkaVersion entryMagic = readable.take(16);
        if (entryMagic != magic) {
            ythrow yexception() << "compressed Kafka legacy record magic " << entryMagic
                << " does not match wrapper magic " << magic;
        }

        auto& entry = entries.emplace_back();
        entry.Read(readable, magic);
    }

    return entries;
}

void AppendLegacyRecord(
    TKafkaRecordBatch& batch,
    const TKafkaRecordBatchV0& entry,
    i64 offset,
    std::optional<i64> timestamp = std::nullopt)
{
    auto& record = batch.Records.emplace_back();
    record.Length = entry.Record.MessageSize;
    record.OffsetDelta = offset;
    record.TimestampDelta = timestamp.value_or(entry.Record.Timestamp);
    if (entry.Record.Key) {
        record.SetKey(TString(entry.Record.Key->data(), entry.Record.Key->size()));
    }
    if (entry.Record.Value) {
        record.SetValue(TString(entry.Record.Value->data(), entry.Record.Value->size()));
    }
}

void AppendLegacyRecords(
    TKafkaRecordBatch& batch,
    const std::vector<TKafkaRecordBatchV0>& entries,
    TKafkaVersion magic,
    i64 wrapperOffset = 0,
    std::optional<i64> wrapperTimestamp = std::nullopt)
{
    if (entries.empty()) {
        return;
    }

    i64 absoluteBaseOffset = 0;
    if (magic == 1) {
        absoluteBaseOffset = wrapperOffset == 0 ? 0 : wrapperOffset - entries.back().Offset;
    }

    for (const auto& entry : entries) {
        const i64 offset = magic == 1 ? absoluteBaseOffset + entry.Offset : entry.Offset;
        AppendLegacyRecord(batch, entry, offset, wrapperTimestamp);
    }
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
    , BaseSequence(BaseSequenceMeta::Default) {
}

ECompressionType TKafkaRecordBatch::CompressionType() const {
    return static_cast<ECompressionType>(Attributes & 0x07);
}

ETimestampType TKafkaRecordBatch::TimestampType() {
    return (Attributes & 0x08) ? ETimestampType::LOG_APPEND_TIME : ETimestampType::CREATE_TIME;
}

bool TKafkaRecordBatch::Transactional() {
    return Attributes & 0x10;
}

bool TKafkaRecordBatch::ControlBatch() {
    return Attributes & 0x20;
}

bool TKafkaRecordBatch::HasDeleteHorizonMs() {
    return Attributes & 0x40;
}

void TKafkaRecordBatch::Compress(TKafkaVersion version) {
    Y_UNUSED(version);
    const auto compressionType = CompressionType();
    EnsureSupportedCompressionType(compressionType);
}

void TKafkaRecordBatch::Decompress(TKafkaVersion version) {
    Y_UNUSED(version);
    EnsureSupportedCompressionType(CompressionType());
}

void TKafkaRecordBatch::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
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
    if (CompressionType() != ECompressionType::NONE && !_readable.GetAllowCompressed()) {
        ythrow yexception() << "Supported only CompressionType::NONE";
    }
    EnsureSupportedCompressionType(CompressionType());

    NPrivate::Read<LastOffsetDeltaMeta>(_readable, _version, LastOffsetDelta);
    NPrivate::Read<BaseTimestampMeta>(_readable, _version, BaseTimestamp);
    NPrivate::Read<MaxTimestampMeta>(_readable, _version, MaxTimestamp);
    NPrivate::Read<ProducerIdMeta>(_readable, _version, ProducerId);
    NPrivate::Read<ProducerEpochMeta>(_readable, _version, ProducerEpoch);
    NPrivate::Read<BaseSequenceMeta>(_readable, _version, BaseSequence);
    RecordsMeta::Type().swap(Records);

    if (CompressionType() == ECompressionType::NONE) {
        NPrivate::Read<RecordsMeta>(_readable, _version, Records);
        for (auto& record : Records) {
            record.OwnPayload();
        }
    } else {
        const TKafkaInt32 recordsCount = NPrivate::ReadArraySize<RecordsMeta>(_readable, _version);
        EnsureValidRecordBatchRecordsCount(recordsCount);
        const auto compressed = _readable.Bytes(_readable.left());
        const TString decompressed = DecompressRecordBatchPayload(
            TStringBuf(compressed.data(), compressed.size()), CompressionType());
        TBuffer buffer(decompressed.data(), decompressed.size());
        TKafkaReadable recordsReadable(buffer);
        Records.resize(recordsCount);
        using ItemStrategy = NPrivate::TypeStrategy<
            RecordsMeta,
            RecordsMeta::ItemType,
            RecordsMeta::ItemTypeDesc>;
        for (auto& record : Records) {
            ItemStrategy::DoRead(recordsReadable, _version, record);
        }
        for (auto& record : Records) {
            record.OwnPayload();
        }
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
        const TString packedRecords = CompressRecordBatchPayload(
            SerializeRecordBatchRecords(Records, _version, false), CompressionType());
        NPrivate::WriteArraySize<RecordsMeta>(_writable, _version, Records.size());
        _writable.write(packedRecords.data(), packedRecords.size());
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
        const TString packedRecords = CompressRecordBatchPayload(
            SerializeRecordBatchRecords(Records, _version, false), CompressionType());
        _collector.Size += NPrivate::ArraySize<RecordsMeta>(_version, Records.size()) + packedRecords.size();
    }

    return _collector.Size;
}



//
// TKafkaRecordV0
//
const TKafkaRecordV0::KeyMeta::Type TKafkaRecordV0::KeyMeta::Default = std::nullopt;

ECompressionType TKafkaRecordV0::CompressionType() const {
    return static_cast<ECompressionType>(Attributes & 0x07);
}

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

void NPrivate::ReadLegacyRecordBatch(
    TKafkaReadable& readable,
    TKafkaVersion magic,
    size_t length,
    TKafkaRecordBatch& batch)
{
    const auto data = readable.Bytes(length);
    TBuffer buffer(data.data(), data.size());
    TKafkaReadable recordsReadable(buffer);

    batch = {};
    batch.Magic = 2;
    batch.BaseOffset = 0;
    batch.BaseTimestamp = 0;

    while (recordsReadable.left() > 0) {
        const TKafkaVersion entryMagic = recordsReadable.take(16);
        if (entryMagic != magic) {
            ythrow yexception() << "Kafka legacy record magic " << entryMagic << " does not match expected magic " << magic;
        }

        TKafkaRecordBatchV0 entry;
        entry.Read(recordsReadable, magic);

        const ECompressionType compressionType = entry.Record.CompressionType();
        if (compressionType == ECompressionType::NONE) {
            AppendLegacyRecord(batch, entry, entry.Offset);
            continue;
        }

        if (!readable.GetAllowCompressed()) {
            ythrow yexception() << "Supported only CompressionType::NONE";
        }
        EnsureSupportedCompressionType(compressionType);
        if (!entry.Record.Value) {
            ythrow yexception() << "compressed Kafka legacy record has null value";
        }

        batch.Attributes = static_cast<TKafkaRecordBatch::AttributesMeta::Type>(compressionType);
        const auto& value = *entry.Record.Value;
        const TString decompressed = DecompressRecordBatchPayload(
            TStringBuf(value.data(), value.size()),
            compressionType);
        TBuffer innerBuffer(decompressed.data(), decompressed.size());
        const std::vector<TKafkaRecordBatchV0> innerEntries = ReadLegacyRecordEntries(innerBuffer, magic);
        const std::optional<i64> wrapperTimestamp = magic == 1
            ? std::optional<i64>(entry.Record.Timestamp)
            : std::nullopt;
        AppendLegacyRecords(batch, innerEntries, magic, entry.Offset, wrapperTimestamp);
    }

    batch.LastOffsetDelta = batch.Records.empty() ? 0 : batch.Records.back().OffsetDelta;
}

TKafkaRecordBatch ReadKafkaRecordBatch(TStringBuf data, TKafkaVersion version) {
    TBuffer buffer(data.data(), data.size());
    TKafkaReadable readable(buffer);
    readable.SetAllowCompressed(true);

    TKafkaRecordBatch batch;
    batch.Read(readable, version);
    if (readable.left() != 0) {
        ythrow yexception() << "unexpected extra bytes after Kafka record batch: " << readable.left();
    }
    return batch;   
}

TString WriteKafkaRecordBatch(const TKafkaRecordBatch& batch, TKafkaVersion version) {
    TKafkaWriteBuffer buffer(WriteBufferChunkSize);
    TKafkaWritable writable(buffer);
    batch.Write(writable, version);
    return buffer.AsString();
}
} // namespace NKafka
