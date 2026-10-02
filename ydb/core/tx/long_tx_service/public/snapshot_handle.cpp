#include "snapshot_handle.h"
#include "events.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NKqp {

TSnapshotHandle::TSnapshotHandle(
    const std::shared_ptr<std::atomic<bool>>& aliveFlag)
    : Alive_(aliveFlag)
{
}

TSnapshotHandle::~TSnapshotHandle() {
    if (Alive_) {
        Reset();
    }
}

TSnapshotHandle::TSnapshotHandle(TSnapshotHandle&& other) noexcept {
    std::swap(Alive_, other.Alive_);
}

TSnapshotHandle& TSnapshotHandle::operator=(TSnapshotHandle&& other) noexcept {
    std::swap(Alive_, other.Alive_);
    return *this;
}

void TSnapshotHandle::Reset() {
    AFL_ENSURE(Alive_);
    Alive_->store(false);
}

} // namespace NKikimr::NKqp