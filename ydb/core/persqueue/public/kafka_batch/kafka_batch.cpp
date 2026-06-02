#include "kafka_batch.h"

#include <util/generic/yexception.h>
#include <util/string/cast.h>

namespace NKikimr::NPQ::NKafkaBatch {
namespace {

constexpr i8 KRecordBatchMagic = 2;
constexpr i16 KCompressionMask = 0x07;
constexpr i64 KSequenceModulo = static_cast<i64>(Max<i32>()) + 1;

class TReader {
public:
    explicit TReader(TStringBuf data)
        : Data(data)
    {
    }

    size_t Position() const {
        return Pos;
    }

    size_t Size() const {
        return Data.size();
    }

    bool Empty() const {
        return Pos == Data.size();
    }

    i8 ReadInt8() {
        return static_cast<i8>(ReadUi8());
    }

    i16 ReadInt16() {
        return static_cast<i16>(ReadUnsignedFixed<ui16>());
    }

    i32 ReadInt32() {
        return static_cast<i32>(ReadUnsignedFixed<ui32>());
    }

    i64 ReadInt64() {
        return static_cast<i64>(ReadUnsignedFixed<ui64>());
    }

    i32 ReadVarInt32() {
        return ReadVarInt<i32>();
    }

    i64 ReadVarInt64() {
        return ReadVarInt<i64>();
    }

    std::optional<TString> ReadVarBytes() {
        const i32 length = ReadVarInt32();
        if (length < 0) {
            return std::nullopt;
        }
        return TString(ReadBytes(length));
    }

    TStringBuf ReadBytes(size_t length) {
        CheckAvailable(length);
        const auto result = Data.SubStr(Pos, length);
        Pos += length;
        return result;
    }

    void CheckConsumed(size_t expectedPosition, TStringBuf what) const {
        if (Pos != expectedPosition) {
            ythrow yexception() << what << " size mismatch: expected end at " << expectedPosition << ", got " << Pos;
        }
    }

private:
    ui8 ReadUi8() {
        CheckAvailable(1);
        return static_cast<ui8>(Data[Pos++]);
    }

    template <typename T>
    T ReadUnsignedFixed() {
        T value = 0;
        for (size_t i = 0; i < sizeof(T); ++i) {
            value = static_cast<T>((value << 8) | ReadUi8());
        }
        return value;
    }

    template <typename S, typename U = std::make_unsigned_t<S>>
    S ReadVarInt() {
        U value = 0;
        size_t shift = 0;
        while (true) {
            const U byte = ReadUi8();
            if (shift >= sizeof(U) * 8) {
                ythrow yexception() << "illegal varint length";
            }
            value |= (byte & 0x7f) << shift;
            if ((byte & 0x80) == 0) {
                break;
            }
            shift += 7;
        }
        return static_cast<S>((value >> 1) ^ (~(value & 1) + 1));
    }

    void CheckAvailable(size_t length) const {
        if (length > Data.size() - Pos) {
            ythrow yexception() << "not enough data: need " << length << " bytes at " << Pos << ", size " << Data.size();
        }
    }

private:
    TStringBuf Data;
    size_t Pos = 0;
};

ui64 AddSequenceDelta(i32 baseSequence, ui64 index) {
    if (baseSequence < 0) {
        return 0;
    }
    return (static_cast<ui64>(baseSequence) + index) % KSequenceModulo;
}

TVector<TKafkaHeader> ReadHeaders(TReader& reader) {
    const i32 count = reader.ReadVarInt32();
    if (count < 0) {
        return {};
    }

    TVector<TKafkaHeader> headers;
    headers.reserve(count);
    for (i32 i = 0; i < count; ++i) {
        headers.push_back({
            .Key = reader.ReadVarBytes(),
            .Value = reader.ReadVarBytes(),
        });
    }
    return headers;
}

TKafkaBatchRecord ReadRecord(TReader& reader, const TKafkaBatch& batch, ui64 recordIndex) {
    const i32 length = reader.ReadVarInt32();
    if (length < 0) {
        ythrow yexception() << "negative Kafka record length: " << length;
    }

    const size_t recordEnd = reader.Position() + length;
    TKafkaBatchRecord record;
    record.Attributes = reader.ReadInt8();

    const i64 timestampDelta = reader.ReadVarInt64();
    const i64 offsetDelta = reader.ReadVarInt64();

    record.Offset = batch.BaseOffset + offsetDelta;
    record.Sequence = AddSequenceDelta(batch.BaseSequence, recordIndex);
    record.Timestamp = batch.BaseTimestamp + timestampDelta;
    record.Key = reader.ReadVarBytes();
    record.Value = reader.ReadVarBytes();
    record.Headers = ReadHeaders(reader);

    reader.CheckConsumed(recordEnd, "Kafka record");
    return record;
}

} // namespace

TKafkaBatch ParseKafkaBatch(TStringBuf data) {
    TReader reader(data);

    TKafkaBatch batch;
    batch.BaseOffset = reader.ReadInt64();
    const i32 batchLength = reader.ReadInt32();
    if (batchLength < 0) {
        ythrow yexception() << "negative Kafka batch length: " << batchLength;
    }

    const size_t batchEnd = reader.Position() + batchLength;
    if (batchEnd > reader.Size()) {
        ythrow yexception() << "Kafka batch length exceeds input size: batchEnd " << batchEnd << ", size " << reader.Size();
    }

    Y_UNUSED(reader.ReadInt32()); // partitionLeaderEpoch
    const i8 magic = reader.ReadInt8();
    if (magic != KRecordBatchMagic) {
        ythrow yexception() << "unsupported Kafka record batch magic: " << static_cast<int>(magic);
    }

    Y_UNUSED(reader.ReadInt32()); // crc
    batch.Attributes = reader.ReadInt16();
    if ((batch.Attributes & KCompressionMask) != 0) {
        ythrow yexception() << "compressed Kafka record batches are not supported yet, attributes: " << batch.Attributes;
    }

    batch.LastOffsetDelta = reader.ReadInt32();
    batch.BaseTimestamp = reader.ReadInt64();
    batch.MaxTimestamp = reader.ReadInt64();
    batch.ProducerId = reader.ReadInt64();
    batch.ProducerEpoch = reader.ReadInt16();
    batch.BaseSequence = reader.ReadInt32();

    const i32 recordsCount = reader.ReadInt32();
    if (recordsCount < 0) {
        ythrow yexception() << "negative Kafka records count: " << recordsCount;
    }

    batch.Records.reserve(recordsCount);
    for (i32 i = 0; i < recordsCount; ++i) {
        batch.Records.push_back(ReadRecord(reader, batch, i));
    }

    reader.CheckConsumed(batchEnd, "Kafka batch");
    if (!reader.Empty()) {
        ythrow yexception() << "unexpected trailing bytes after Kafka batch: " << (reader.Size() - reader.Position());
    }

    return batch;
}

TVector<TKafkaBatchRecord> ParseKafkaBatchRecords(TStringBuf data) {
    return ParseKafkaBatch(data).Records;
}

} // namespace NKikimr::NPQ::NKafkaBatch
