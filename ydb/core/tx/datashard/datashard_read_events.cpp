#include "datashard.h"

#include <util/stream/str.h>
#include <util/system/unaligned_mem.h>

namespace NKikimr {

struct TEvDataShard::TEvRead::TReadData {
    struct TEndpoint {
        size_t Offset;
        ui32 RawSize;
        ui32 Size;
    };

    size_t KeyColumnCount = 0;
    TVector<TEndpoint> Endpoints;
    TVector<TCell> Cells;

    bool Parse(const NKikimrTxDataShard::TEvRead& record, size_t keyColumnCount) {
        if (keyColumnCount > Max<ui32>()) {
            return false;
        }
        KeyColumnCount = keyColumnCount;
        Endpoints.reserve(size_t(record.KeysSize()) + 2 * size_t(record.RangesSize()));
        size_t total = 0;
        auto append = [&](const TString& buffer, bool pad) {
            ui32 count = 0;
            if (!buffer.empty()) {
                if (buffer.size() < sizeof(ui16)) {
                    return false;
                }
                count = ReadUnaligned<ui16>(buffer.data());
                if (count > (buffer.size() - sizeof(ui16)) / sizeof(ui32)) {
                    return false;
                }
            }
            const ui32 size = pad ? static_cast<ui32>(keyColumnCount) : count;
            const size_t capacity = Max(count, size);
            if (capacity > Max<size_t>() / sizeof(TCell) - total) {
                return false;
            }
            Endpoints.push_back({ total, count, size });
            total += capacity;
            return true;
        };
        for (const auto& key : record.GetKeys()) {
            if (!append(key, true)) {
                return false;
            }
        }
        for (const auto& range : record.GetRanges()) {
            if (!append(range.GetFrom(), range.GetFromInclusive()) || !append(range.GetTo(), !range.GetToInclusive())) {
                return false;
            }
        }
        Cells.resize_uninitialized(total);
        size_t index = 0;
        auto decode = [&](const TString& buffer) {
            const auto& endpoint = Endpoints[index++];
            TCell* ptr = total ? Cells.data() + endpoint.Offset : nullptr;
            if (!TSerializedCellVec::TryParseCellsInto(
                    std::string_view(buffer.data(), buffer.size()), TArrayRef<TCell>(ptr, endpoint.RawSize))) {
                return false;
            }
            for (size_t i = endpoint.RawSize; i < endpoint.Size; ++i) {
                new (ptr + i) TCell();
            }
            return true;
        };
        for (const auto& key : record.GetKeys()) {
            if (!decode(key)) {
                return false;
            }
        }
        for (const auto& range : record.GetRanges()) {
            if (!decode(range.GetFrom()) || !decode(range.GetTo())) {
                return false;
            }
        }
        return true;
    }

    NDataShard::TReadCellVecView Get(size_t index, const TString& buffer, bool raw = false) const {
        const auto& endpoint = Endpoints.at(index);
        const size_t size = raw ? endpoint.RawSize : endpoint.Size;
        const TCell* ptr = Cells.empty() ? nullptr : Cells.data() + endpoint.Offset;
        return { TConstArrayRef<TCell>(ptr, size), size == endpoint.RawSize ? &buffer : nullptr };
    }
};

TEvDataShard::TEvRead::TEvRead() = default;
TEvDataShard::TEvRead::~TEvRead() = default;

size_t TEvDataShard::TEvRead::GetKeysSize() const {
    return CompactReadData ? size_t(Record.KeysSize()) : Keys.size();
}

size_t TEvDataShard::TEvRead::GetRangesSize() const {
    return CompactReadData ? size_t(Record.RangesSize()) : Ranges.size();
}

NDataShard::TReadCellVecView TEvDataShard::TEvRead::GetKey(size_t index) const {
    if (!CompactReadData) {
        return Keys.at(index);
    }
    Y_ENSURE(ReadData, "Read data must be prepared before accessing cells");
    return ReadData->Get(index, Record.GetKeys(index), true);
}

NDataShard::TReadCellVecView TEvDataShard::TEvRead::GetKeyLowerBound(size_t index) const {
    Y_ENSURE(ReadData, "Read data must be prepared before accessing cells");
    return ReadData->Get(index, Record.GetKeys(index));
}

NDataShard::TReadRangeView TEvDataShard::TEvRead::GetRange(size_t index) const {
    if (!CompactReadData) {
        return Ranges.at(index);
    }
    Y_ENSURE(ReadData, "Read data must be prepared before accessing cells");
    const auto& range = Record.GetRanges(index);
    const size_t offset = size_t(Record.KeysSize()) + 2 * index;
    NDataShard::TReadRangeView result;
    result.From = ReadData->Get(offset, range.GetFrom());
    result.To = ReadData->Get(offset + 1, range.GetTo());
    result.FromInclusive = range.GetFromInclusive();
    result.ToInclusive = range.GetToInclusive();
    return result;
}

bool TEvDataShard::TEvRead::PrepareReadData(size_t keyColumnCount) {
    if (!CompactReadData) {
        return true;
    }
    if (ReadData) {
        return ReadData->KeyColumnCount == keyColumnCount;
    }
    auto data = MakeHolder<TReadData>();
    if (!data->Parse(Record, keyColumnCount)) {
        return false;
    }
    ReadData = std::move(data);
    return true;
}

TString TEvDataShard::TEvRead::ToString() const {
    TStringStream ss;
    ss << TBase::ToString();
    if (GetKeysSize()) {
        ss << " KeysSize: " << GetKeysSize();
    }
    if (GetRangesSize()) {
        ss << " RangesSize: " << GetRangesSize();
    }
    return ss.Str();
}

TEvDataShard::TEvRead* TEvDataShard::TEvRead::Load(const TEventSerializedData* data) {
    THolder<TEvRead> event(TBase::Load(data));
    const auto& record = event->Record;
    // Preserve the small-read path; large reads avoid per-endpoint cell vectors.
    constexpr size_t CompactReadMinEntries = 128;
    if (size_t(record.KeysSize()) + size_t(record.RangesSize()) >= CompactReadMinEntries) {
        event->CompactReadData = true;
        return event.Release();
    }
    event->Keys.reserve(record.KeysSize());
    for (const auto& key : record.GetKeys()) {
        event->Keys.emplace_back(key);
    }
    event->Ranges.reserve(record.RangesSize());
    for (const auto& range : record.GetRanges()) {
        event->Ranges.emplace_back(range);
    }
    return event.Release();
}

// CalculateSerializedSize also materializes the typed producer representation.
void TEvDataShard::TEvRead::FillRecord() {
    if (CompactReadData) {
        return;
    }
    if (!Keys.empty()) {
        Record.ClearKeys();
        Record.MutableKeys()->Reserve(Keys.size());
        for (auto& key : Keys) {
            Record.AddKeys(key.ReleaseBuffer());
        }
        Keys.clear();
    }
    if (!Ranges.empty()) {
        Record.ClearRanges();
        Record.MutableRanges()->Reserve(Ranges.size());
        for (auto& range : Ranges) {
            range.Serialize(*Record.AddRanges());
        }
        Ranges.clear();
    }
}

} // namespace NKikimr
