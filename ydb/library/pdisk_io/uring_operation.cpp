#include "uring_operation.h"

namespace NKikimr::NPDisk {

TUringOperationBase::~TUringOperationBase() = default;

void TUringOperationBase::PrepareIov(void* buf, size_t size, ui64 offset) {
    // additional calls are normally from AdvanceIov()
    if (TotalSize == 0) {
        TotalSize = size;
    }

    DiskOffset = offset;

#if defined(__linux__)
    Iov.iov_base = buf;
    Iov.iov_len = size;
#endif
}

void TUringOperationBase::AdvanceIov(size_t bytesProcessed) {
    // when non linux or non uring - we don't have short reads/writes,
    // so having NOP is OK
#if defined(__linux__)
    auto* nextBuffer = static_cast<char*>(Iov.iov_base) + bytesProcessed;
    const size_t nextSize = Iov.iov_len - bytesProcessed;
    const ui64 nextOffset = DiskOffset + bytesProcessed;
    PrepareIov(nextBuffer, nextSize, nextOffset);
#endif
}

} // namespace NKikimr::NPDisk
