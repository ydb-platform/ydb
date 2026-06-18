#include "uring_operation.h"

#include <util/system/compiler.h>
#include <util/system/yassert.h>

namespace NKikimr::NPDisk {

TUringOperationBase::~TUringOperationBase() = default;

void TUringOperationBase::PrepareIov(void* buf, size_t size, ui64 offset) {
    if (TotalSize == 0) {
        TotalSize = size;
    }

    DiskOffset = offset;

#if defined(__linux__)
    Iov.clear();
    Iov.push_back({buf, size});
    IovBegin = 0;
#else
    Y_UNUSED(buf);
#endif
}

#if defined(__linux__)
void TUringOperationBase::PrepareScatterGather(size_t count, ui64 offset) {
    Y_ABORT_UNLESS(count > 0 && count <= MAX_IOVS);

    TotalSize = 0;
    DiskOffset = offset;

    Iov.clear();
    Iov.reserve(count);
    IovBegin = 0;
}

void TUringOperationBase::AddIov(void* buf, size_t size) {
    Y_ABORT_UNLESS(Iov.size() < MAX_IOVS);
    TotalSize += size;
    Iov.push_back({buf, size});
}
#endif

void TUringOperationBase::AdvanceIov(size_t bytesProcessed) {
    // On non-Linux there are no short reads/writes via io_uring, so NOP is fine.
#if defined(__linux__)
    DiskOffset += bytesProcessed;

    // Consume whole iovecs first.
    while (bytesProcessed > 0 && IovBegin < Iov.size()) {
        if (bytesProcessed >= Iov[IovBegin].iov_len) {
            bytesProcessed -= Iov[IovBegin].iov_len;
            ++IovBegin;
        } else {
            // Partial iovec: trim from the front.
            Iov[IovBegin].iov_base = static_cast<char*>(Iov[IovBegin].iov_base) + bytesProcessed;
            Iov[IovBegin].iov_len -= bytesProcessed;
            bytesProcessed = 0;
        }
    }
#else
    Y_UNUSED(bytesProcessed);
#endif
}

} // namespace NKikimr::NPDisk
