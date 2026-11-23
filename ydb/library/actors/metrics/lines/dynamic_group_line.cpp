#include "dynamic_group_line.h"
#include <algorithm>

namespace NActors {
namespace {
    using TCodec = TDynamicGroupFrontend::TCodec;
    constexpr size_t MaxFields = TDynamicGroupSchema::MaxFields;
    struct THeader {
        ui32 Version;
        ui32 Fields;
        ui64 LastTimestamp; // Writer-only cache; snapshot readers ignore it.
    };
    constexpr size_t HeaderBytes(size_t fields) { return sizeof(THeader) + fields * sizeof(ui64); }
    struct TRecord {
        const TDynamicGroupSchema* Schema;
        NHPTimer::STime Timestamp;
        std::array<ui64, MaxFields> Values;
    };

    bool Encode(EGroupValueType type, const TLineNumericValue& value, ui64* encoded) noexcept {
        switch (type) {
            case EGroupValueType::Unsigned:
                if (const auto* v = std::get_if<ui64>(&value)) { *encoded = *v; return true; }
                break;
            case EGroupValueType::Signed:
                if (const auto* v = std::get_if<i64>(&value)) { *encoded = static_cast<ui64>(*v); return true; }
                break;
            case EGroupValueType::Bool:
                if (const auto* v = std::get_if<bool>(&value)) { *encoded = *v; return true; }
                break;
            case EGroupValueType::Decimal:
                if (const auto* v = std::get_if<double>(&value)) return TDecimalEncoding<2>::Encode(*v, encoded);
                break;
        }
        return false;
    }
    TLineNumericValue Decode(EGroupValueType type, ui64 value) {
        switch (type) {
            case EGroupValueType::Unsigned: return value;
            case EGroupValueType::Signed: return static_cast<i64>(value);
            case EGroupValueType::Bool: return bool(value);
            case EGroupValueType::Decimal: return TDecimalEncoding<2>::Decode<double>(value);
        }
        Y_UNREACHABLE();
    }

    bool Write(void* opaque, TWritableChunkMemory& memory) noexcept {
        const auto& record = *static_cast<const TRecord*>(opaque);
        if (record.Timestamp < 0 || ui64(record.Timestamp) >= (ui64(1) << 62)) return false;
        const size_t count = record.Schema->Definitions.size(), headerBytes = HeaderBytes(count);
        if (memory.Payload.size() < headerBytes) return false;
        const bool first = memory.UsedPayloadBytes == 0;
        auto* header = reinterpret_cast<THeader*>(memory.Payload.data());
        auto* previousValues = reinterpret_cast<ui64*>(memory.Payload.data() + sizeof(THeader));
        const ui64 step = TCodec::TimestampStep(), timestamp = ui64(record.Timestamp) / step * step;
        const ui64 previous = first ? 0 : header->LastTimestamp;
        const ui64 delta = timestamp >= previous ? (timestamp - previous) / step : ui64(-1);
        const bool absolute = first || timestamp < previous || delta >= (ui64(1) << 30);
        const bool partial = !first && record.Schema->Mode == EGroupUpdateMode::OnChangePartial;
        std::array<char, 11 + 10 * MaxFields> buffer;
        buffer[0] = partial ? 1 : 0;
        size_t size = 1 + TCodec::Pack(absolute ? timestamp : delta, buffer.data() + 1, absolute);
        size_t changed = 0, countOffset = size;
        if (partial) size += 2;
        for (size_t i = 0; i < count; ++i) {
            if (partial && previousValues[i] == record.Values[i]) continue;
            if (partial) buffer[size++] = static_cast<char>(i);
            size += TCodec::PackValue<0>(record.Values[i], first ? 0 : previousValues[i], first, buffer.data() + size);
            ++changed;
        }
        if (partial) {
            buffer[countOffset] = static_cast<char>(changed);
            buffer[countOffset + 1] = static_cast<char>(changed >> 8);
        }
        const size_t offset = first ? headerBytes : memory.UsedPayloadBytes;
        if (offset + size > memory.Payload.size()) return false;
        if (first) *header = {1, static_cast<ui32>(count), timestamp};
        std::memcpy(memory.Payload.data() + offset, buffer.data(), size);
        header->LastTimestamp = timestamp;
        std::copy_n(record.Values.begin(), count, previousValues);
        memory.UsedPayloadBytes = offset + size;
        if (first) memory.FirstTs = record.Timestamp;
        memory.LastTs = record.Timestamp;
        return true;
    }

    template<class TCallback>
    void ReadRecords(const TLineSnapshot& snapshot, const TDynamicGroupSchema& schema, TCallback&& callback) {
        const size_t count = schema.Definitions.size();
        NInMemoryMetricsPrivate::TLineSnapshotAccess::ForEachChunk(snapshot, [&](const auto& chunk) {
            const size_t headerBytes = HeaderBytes(count);
            if (chunk.Payload.size() < headerBytes) return;
            ui32 version, fields;
            std::memcpy(&version, chunk.Payload.data(), 4);
            std::memcpy(&fields, chunk.Payload.data() + 4, 4);
            if (version != 1 || fields != count) return;
            const char* cursor = chunk.Payload.data() + headerBytes;
            const char* end = chunk.Payload.data() + chunk.Payload.size();
            std::array<ui64, MaxFields> encoded = {};
            std::array<TLineNumericValue, MaxFields> values;
            ui64 timestamp = 0;
            bool first = true;
            while (cursor < end) {
                const ui8 mode = static_cast<ui8>(*cursor++);
                if (mode > 1 || (first && mode != 0)) return;
                ui64 delta; ui8 tag;
                if (!TCodec::Unpack(&cursor, end, &delta, &tag) || (first && tag != 0)) return;
                if (tag == 0) timestamp = delta;
                else {
                    const ui64 step = TCodec::TimestampStep();
                    if (delta > (((ui64(1) << 62) - 1) - timestamp) / step) return;
                    timestamp += delta * step;
                }
                size_t changed = count;
                if (mode) {
                    if (end - cursor < 2) return;
                    changed = static_cast<ui8>(cursor[0]) | (size_t(static_cast<ui8>(cursor[1])) << 8);
                    cursor += 2;
                    if (changed > count) return;
                }
                size_t previousId = 0;
                for (size_t i = 0; i < changed; ++i) {
                    if (mode && cursor == end) return;
                    const size_t id = mode ? static_cast<ui8>(*cursor++) : i;
                    if (id >= count || (mode && i && id <= previousId)) return;
                    previousId = id;
                    if (first && (cursor == end || *cursor != 0)) return;
                    if (!TCodec::UnpackValue<0>(&cursor, end, encoded[id], &encoded[id])) return;
                }
                for (size_t i = 0; i < count; ++i) values[i] = Decode(schema.Definitions[i].Type, encoded[i]);
                callback(NInMemoryMetricsPrivate::TLineSnapshotAccess::DecodeTimestampTs(snapshot, timestamp),
                    std::span<const TLineNumericValue>(values.data(), count));
                first = false;
            }
        });
    }
} // namespace

TDynamicGroupSchema::TDynamicGroupSchema(TVector<TDynamicGroupField> fields, EGroupUpdateMode mode)
    : Definitions(std::move(fields)), Mode(mode)
{
    Y_ENSURE(!Definitions.empty() && Definitions.size() <= MaxFields, "Dynamic group size out of bounds");
    Name = mode == EGroupUpdateMode::All ? "group" : "on_change";
    ReadNumericRange = &TDynamicGroupFrontend::ReadNumericRange;
    LabelViews.resize(Definitions.size());
    Metadata.reserve(Definitions.size());
    for (size_t i = 0; i < Definitions.size(); ++i) {
        Y_ENSURE(!Definitions[i].Name.empty());
        for (const auto& label : Definitions[i].Labels) LabelViews[i].push_back({label.Name, label.Value});
        Metadata.push_back({Definitions[i].Name, LabelViews[i]});
    }
    Fields = Metadata;
}

bool TDynamicGroupFrontend::Append(IMetricLine& line, const TValueType& value) noexcept {
    if (!line.IsValid() || !value.Schema || !value.Cache || value.Values.size() != value.Schema->Definitions.size()) return false;
    TRecord record{value.Schema, line.CurrentTimestampTs(), {}};
    // The endpoint invalidates materialization when its last record is evicted
    // or closed. A private value cache alone must not suppress rematerialization.
    bool changed = !value.Cache->Valid
        || (value.Schema->Mode != EGroupUpdateMode::All && !line.GetLastMaterializedValue());
    for (size_t i = 0; i < value.Values.size(); ++i) {
        if (!Encode(value.Schema->Definitions[i].Type, value.Values[i], &record.Values[i])) return false;
        changed |= record.Values[i] != value.Cache->Values[i];
    }
    if (value.Schema->Mode != EGroupUpdateMode::All && !changed) return true;
    if (!line.AccessChunkMemory(&record, Write)) return false;
    value.Cache->Values = record.Values;
    value.Cache->Valid = true;
    if (value.Schema->Mode != EGroupUpdateMode::All) line.MarkMaterialized(1);
    return true;
}

void TDynamicGroupFrontend::ReadNumericRange(const TLineSnapshot& snapshot, TInstant begin, TInstant end,
        void* opaque, TLineFrontendOps::TInvokeNumericValues invoke) {
    const auto& schema = *static_cast<const TDynamicGroupSchema*>(snapshot.Meta.Frontend);
    const bool onChange = schema.Mode != EGroupUpdateMode::All;
    const bool finiteEnd = end != TInstant::Max();
    if (snapshot.Closed) end = std::min(end, snapshot.ClosedAt);
    if (begin > end) return;
    std::array<TLineNumericValue, MaxFields> previous;
    bool havePrevious = false, emitted = false;
    TInstant last;
    ReadRecords(snapshot, schema, [&](TInstant timestamp, std::span<const TLineNumericValue> values) {
        if (timestamp > end) return;
        if (timestamp < begin) {
            // Older records must not replace the last emitted value used for the range tail.
            if (!emitted) {
                std::copy(values.begin(), values.end(), previous.begin());
                havePrevious = true;
            }
            return;
        }
        if (onChange && !emitted && havePrevious && begin < timestamp) invoke(opaque, begin, {previous.data(), values.size()});
        invoke(opaque, timestamp, values);
        std::copy(values.begin(), values.end(), previous.begin());
        havePrevious = emitted = true; last = timestamp;
    });
    if (onChange && havePrevious) {
        const auto values = std::span<const TLineNumericValue>(previous.data(), schema.Definitions.size());
        if (!emitted) { invoke(opaque, begin, values); last = begin; }
        if (finiteEnd && last < end) invoke(opaque, end, values);
    }
}
} // namespace NActors
