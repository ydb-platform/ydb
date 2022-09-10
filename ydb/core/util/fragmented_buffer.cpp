#include "fragmented_buffer.h"

#include <util/stream/str.h>
#include <library/cpp/actors/util/shared_data_rope_backend.h>

namespace NKikimr {

TFragmentedBuffer::TFragmentedBuffer() {
}

void TFragmentedBuffer::Insert(i32 begin, const char* source, i32 bytesToCopy) {
    Y_VERIFY(bytesToCopy);
    //FIXME(innokentii): can save allocation
    BufferForOffset[begin] = TRope(MakeIntrusive<TRopeSharedDataBackend>(TSharedData::Copy(source, bytesToCopy)));
}


bool TFragmentedBuffer::IsMonolith() const {
    return (BufferForOffset.size() == 1 && BufferForOffset.begin()->first == 0);
}

TRope TFragmentedBuffer::GetMonolith() {
    Y_VERIFY(IsMonolith());
    return BufferForOffset.begin()->second;
}

void TFragmentedBuffer::SetMonolith(TRope &data) {
    Y_VERIFY(data);
    BufferForOffset.clear();
    BufferForOffset.emplace(0, data);
}

void TFragmentedBuffer::Write(i32 begin, const char* buffer, i32 size) {
    Y_VERIFY(size);
    if (BufferForOffset.empty()) {
        Insert(begin, buffer, size);
        return;
    }
    auto it = BufferForOffset.upper_bound(begin);
    if (it != BufferForOffset.begin()) {
        it--;
    }
    if (it != BufferForOffset.end() && it->first < begin && it->first + i32(it->second.size()) <= begin) {
        // b....e
        //         X....Y
        // skip
        it++;
    }

    const char* source = buffer;
    i32 bytesToCopy = size;
    i32 offset = begin;
    while (bytesToCopy) {
        if (it == BufferForOffset.end()) {
            Insert(offset, source, bytesToCopy);
            break;
        } else if (it->first > offset) {
            i32 bytesToNext = it->first - offset;
            i32 bytesToInsert = Min(bytesToCopy, bytesToNext);
            Insert(offset, source, bytesToInsert);
            source += bytesToInsert;
            offset += bytesToInsert;
            bytesToCopy -= bytesToInsert;
        } else if (it->first <= offset) {
            Y_VERIFY(it->second.size());
            Y_VERIFY(it->first + i32(it->second.size()) > offset);
            i32 bytesToNext = it->first + it->second.size() - offset;
            i32 bytesToInsert = Min(bytesToCopy, bytesToNext);
            char *destination = it->second.UnsafeGetContiguousSpanMut().data() + offset - it->first;
            memcpy(destination, source, bytesToInsert);
            source += bytesToInsert;
            offset += bytesToInsert;
            bytesToCopy -= bytesToInsert;
            it++;
        }
    }

}

void TFragmentedBuffer::Read(i32 begin, char* buffer, i32 size) const {
    Y_VERIFY(size);
    Y_VERIFY(!BufferForOffset.empty());
    auto it = BufferForOffset.upper_bound(begin);
    if (it != BufferForOffset.begin()) {
        it--;
    }
    if (it != BufferForOffset.end() && it->first < begin && it->first + i32(it->second.size()) <= begin) {
        Y_VERIFY(false);
        // b....e
        //         X....Y
    }

    char* destination = buffer;
    i32 bytesToCopy = size;
    i32 offset = begin;
    while (bytesToCopy) {
        Y_VERIFY(it != BufferForOffset.end(), "offset# %" PRIi32 " Print# %s", (i32)offset, Print().c_str());
        Y_VERIFY(it->first <= offset, "offset# %" PRIi32 " Print# %s", (i32)offset, Print().c_str());
        Y_VERIFY(it->first + i32(it->second.size()) > offset);
        i32 bytesToNext = it->first + it->second.size() - offset;
        i32 bytesToInsert = Min(bytesToCopy, bytesToNext);
        const char *source = const_cast<TRope&>(it->second).GetContiguousSpan().data() + offset - it->first;
        memcpy(destination, source, bytesToInsert);
        destination += bytesToInsert;
        offset += bytesToInsert;
        bytesToCopy -= bytesToInsert;
        it++;
    }
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

std::pair<const char*, i32> TFragmentedBuffer::Get(i32 begin) const {
    auto it = BufferForOffset.upper_bound(begin);
    Y_VERIFY(it != BufferForOffset.begin());
    --it;
    const i32 offset = begin - it->first;
    Y_VERIFY(offset >= 0 && (size_t)offset < it->second.size());
    return std::make_pair(const_cast<TRope&>(it->second).GetContiguousSpan().data() + offset, it->second.size() - offset);
}

void TFragmentedBuffer::CopyFrom(const TFragmentedBuffer& from, const TIntervalSet<i32>& range) {
    Y_VERIFY(range);
    for (auto it = range.begin(); it != range.end(); ++it) {
        auto [begin, end] = *it;
        i32 offset = begin;
        while (offset < end) {
            const auto& [data, maxLen] = from.Get(offset);
            i32 len = Min(maxLen, end - offset);
            Write(offset, data, len);
            offset += len;
        }
    }
}

} // NKikimr
