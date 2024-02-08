#include "fragmented_buffer.h"

#include <util/stream/str.h>
#include <ydb/library/actors/util/shared_data_rope_backend.h>

namespace NKikimr {

bool TFragmentedBuffer::IsMonolith() const {
    return (BufferForOffset.size() == 1 && BufferForOffset.begin()->first == 0);
}

TRope TFragmentedBuffer::GetMonolith() {
    Y_ABORT_UNLESS(IsMonolith());
    return BufferForOffset.begin()->second;
}

void TFragmentedBuffer::SetMonolith(TRope&& data) {
    Y_ABORT_UNLESS(data);
    BufferForOffset.clear();
    BufferForOffset[0] = std::move(data);
}

void TFragmentedBuffer::Write(ui32 begin, const char* buffer, ui32 size) {
    Write(begin, TRcBuf::Copy(buffer, size));
}

void TFragmentedBuffer::Write(ui32 begin, TRope&& data) {
    auto it = BufferForOffset.upper_bound(begin);
    if (it != BufferForOffset.begin()) {
        auto& [prevOffset, prevRope] = *--it;
        if (begin <= prevOffset + prevRope.size()) {
            const ui32 overlap = prevOffset + prevRope.size() - begin;
            if (data.size() < overlap) {
                const ui32 offset = begin - prevOffset;
                prevRope.Erase(prevRope.Position(offset), prevRope.Position(offset + data.size()));
                prevRope.Insert(prevRope.Position(offset), std::exchange(data, {}));
            } else {
                prevRope.EraseBack(overlap);
                prevRope.Insert(prevRope.End(), std::exchange(data, {}));
            }
        } else {
            ++it;
        }
    }

    if (data) {
        it = BufferForOffset.try_emplace(it, begin, std::move(data));
    }

    // consume or join succeeding intervals
    auto& [prevOffset, prevRope] = *it++;
    const ui32 end = prevOffset + prevRope.size();
    auto endIt = BufferForOffset.upper_bound(end);
    Y_DEBUG_ABORT_UNLESS(endIt != BufferForOffset.begin());
    auto& [lastOffset, lastRope] = *std::prev(endIt);
    const ui32 bytesToCut = end - lastOffset;
    if (bytesToCut < lastRope.size()) {
        lastRope.EraseFront(bytesToCut);
        prevRope.Insert(prevRope.End(), std::move(lastRope));
    }
    BufferForOffset.erase(it, endIt);
}

void TFragmentedBuffer::Read(ui32 begin, char* buffer, ui32 size) const {
    Read(begin, size).ExtractFrontPlain(buffer, size);
}

TRope TFragmentedBuffer::Read(ui32 begin, ui32 size) const {
    // X....Y X.....Y X'.....Y'
    //        b.b.e.e
    auto it = BufferForOffset.upper_bound(begin);
    Y_ABORT_UNLESS(it != BufferForOffset.begin());
    --it;
    Y_ABORT_UNLESS(it->first <= begin && begin + size <= it->first + it->second.size());
    const auto iter = it->second.begin() + (begin - it->first);
    return {iter, iter + size};
}

TString TFragmentedBuffer::Print() const {
    TStringStream str;
    str << "{";
    for (auto it = BufferForOffset.begin(); it != BufferForOffset.end(); it++) {
        if (it != BufferForOffset.begin()) {
            str << " U ";
        }
        str << "[" << it->first << ", " << (it->first + it->second.size()) << ")";
    }
    str << "}";
    return str.Str();
}

void TFragmentedBuffer::CopyFrom(const TFragmentedBuffer& from, const TIntervalSet<i32>& range, i32 offset) {
    for (auto [begin, end] : range) {
        Write(begin + offset, from.Read(begin, end - begin));
    }
}

TIntervalSet<i32> TFragmentedBuffer::GetIntervalSet() const {
    TIntervalSet<i32> res;
    for (auto& [offset, buffer] : BufferForOffset) {
        Y_DEBUG_ABORT_UNLESS(buffer);
        res.Add(offset, offset + buffer.size());
    }
    return res;
}

} // NKikimr
