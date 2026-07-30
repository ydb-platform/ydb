#include "fragmented_buffer.h"

#include <util/stream/str.h>
#include <ydb/library/actors/util/shared_data_rope_backend.h>

namespace NKikimr {

bool TFragmentedBuffer::IsMonolith() const {
    return (BufferForOffset.size() == 1 && BufferForOffset.front().first == 0);
}

TRope TFragmentedBuffer::GetMonolith() {
    Y_ABORT_UNLESS(IsMonolith());
    return BufferForOffset.front().second;
}

void TFragmentedBuffer::SetMonolith(TRope&& data) {
    Y_ABORT_UNLESS(data);
    BufferForOffset.clear();
    BufferForOffset.emplace_back(0, std::move(data));
}

void TFragmentedBuffer::Write(ui32 begin, const char* buffer, ui32 size) {
    Write(begin, TRcBuf::Copy(buffer, size));
}

void TFragmentedBuffer::Write(ui32 begin, TRope&& data) {
    // index of the fragment that is going to hold the written data; iterators do not survive the
    // insertion below, so everything here is index-based
    size_t idx = UpperBound(begin);
    if (idx) {
        auto& [prevOffset, prevRope] = BufferForOffset[idx - 1];
        if (begin <= prevOffset + prevRope.size()) {
            --idx; // the preceding fragment touches the written range, so it absorbs the data
            const ui32 overlap = prevOffset + prevRope.size() - begin;
            if (data.size() < overlap) {
                const ui32 offset = begin - prevOffset;
                prevRope.Erase(prevRope.Position(offset), prevRope.Position(offset + data.size()));
                prevRope.Insert(prevRope.Position(offset), std::exchange(data, {}));
            } else {
                prevRope.EraseBack(overlap);
                prevRope.Insert(prevRope.End(), std::exchange(data, {}));
            }
        }
    }

    if (data) {
        BufferForOffset.emplace(BufferForOffset.begin() + idx, begin, std::move(data));
    }

    // consume or join succeeding intervals
    const ui32 end = BufferForOffset[idx].first + BufferForOffset[idx].second.size();
    const size_t endIdx = UpperBound(end);
    Y_DEBUG_ABORT_UNLESS(endIdx != 0);
    auto& [lastOffset, lastRope] = BufferForOffset[endIdx - 1];
    const ui32 bytesToCut = end - lastOffset;
    if (bytesToCut < lastRope.size()) {
        lastRope.EraseFront(bytesToCut);
        auto& rope = BufferForOffset[idx].second;
        rope.Insert(rope.End(), std::move(lastRope));
    }
    BufferForOffset.erase(BufferForOffset.begin() + idx + 1, BufferForOffset.begin() + endIdx);
}

void TFragmentedBuffer::Read(ui32 begin, char* buffer, ui32 size) const {
    Read(begin, size).ExtractFrontPlain(buffer, size);
}

TRope TFragmentedBuffer::Read(ui32 begin, ui32 size) const {
    // X....Y X.....Y X'.....Y'
    //        b.b.e.e
    const size_t idx = UpperBound(begin);
    Y_ABORT_UNLESS(idx != 0);
    const auto& [offset, rope] = BufferForOffset[idx - 1];
    Y_ABORT_UNLESS(offset <= begin && begin + size <= offset + rope.size());
    const auto iter = rope.begin() + (begin - offset);
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
