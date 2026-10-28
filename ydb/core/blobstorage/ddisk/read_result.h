#pragma once

#include <ydb/library/actors/util/rope.h>
#include <util/generic/array_ref.h>
#include <cstring>
#include <variant>
#include <contrib/restricted/abseil-cpp/absl/container/inlined_vector.h>

namespace NKikimr::NDDisk {

// Checksum snapshots own their storage; the common singleton stays inline.
using TReadChecksums = absl::InlinedVector<ui64, 1>;

// Native buffers remain native until the client reply needs a rope.
class TReadPayload {
    std::variant<std::monostate, TRcBuf, TRope> Storage;
public:
    TReadPayload() = default;

    TReadPayload(TRcBuf data) : Storage(std::move(data)) {
    }

    TReadPayload(TRope data) : Storage(std::move(data)) {
    }

    bool IsNative() const {
        return std::holds_alternative<TRcBuf>(Storage);
    }

    size_t size() const {
        if (const auto* data = std::get_if<TRcBuf>(&Storage)) {
            return data->size();
        }
        if (const auto* data = std::get_if<TRope>(&Storage)) {
            return data->size();
        }
        return 0;
    }

    TArrayRef<char> MutableSpan() {
        if (auto* data = std::get_if<TRcBuf>(&Storage)) {
            return {data->GetDataMut(), data->size()};
        }
        if (auto* data = std::get_if<TRope>(&Storage)) {
            return data->UnsafeGetContiguousSpanMut();
        }
        return {};
    }

    void CopyTo(void* destination, size_t size) const {
        Y_ABORT_UNLESS(size == this->size());
        if (const auto* data = std::get_if<TRcBuf>(&Storage)) {
            memcpy(destination, data->GetData(), size);
        }
        else if (const auto* data = std::get_if<TRope>(&Storage)) {
            data->Begin().ExtractPlainDataAndAdvance(destination, size);
        }
    }

    TRope IntoRope() && {
        TRope result;
        if (auto* data = std::get_if<TRcBuf>(&Storage)) {
            result = TRope(std::move(*data));
        }
        else if (auto* data = std::get_if<TRope>(&Storage)) {
            result = std::move(*data);
        }
        Storage.emplace<std::monostate>();
        return result;
    }
};

} // namespace NKikimr::NDDisk
