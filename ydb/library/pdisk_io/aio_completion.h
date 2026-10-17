#pragma once

#include <util/system/defaults.h>

#include <algorithm>
#include <cerrno>
#include <limits>

namespace NKikimr::NPDisk::NDetail {

// A successful device completion must cover the entire request. Zero bytes are
// only successful for the zero-length read used as a PDisk completion barrier.
inline i64 CheckIoCompletion(i64 result, ui64 requestedSize) {
    if (result >= 0 && static_cast<ui64>(result) != requestedSize) {
        return -EIO;
    }
    return result;
}

// The writer returns a byte count or a negative error code. TFileHandle already
// retries EINTR on Unix. Continue short writes without skipping any bytes.
template <class TWriter>
i64 WriteAll(const void* data, ui64 size, ui64 offset, TWriter&& writer) {
    const ui64 requestedSize = size;
    auto* next = static_cast<const ui8*>(data);
    while (size) {
        const ui32 partSize = static_cast<ui32>(std::min<ui64>(size, std::numeric_limits<i32>::max()));
        const i64 result = writer(next, partSize, offset);
        if (result < 0) {
            return result;
        }
        if (result == 0 || static_cast<ui64>(result) > partSize) {
            return -EIO;
        }
        next += result;
        offset += result;
        size -= result;
    }
    return requestedSize;
}

} // namespace NKikimr::NPDisk::NDetail
