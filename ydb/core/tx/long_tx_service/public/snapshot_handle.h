#pragma once

#include <ydb/core/base/row_version.h>
#include <ydb/library/actors/core/actorid.h>
#include <util/generic/vector.h>
#include <atomic>
#include <memory>

namespace NKikimr::NKqp {

class TSnapshotHandle {
public:
    TSnapshotHandle() = default;
    TSnapshotHandle(
        const std::shared_ptr<std::atomic<bool>>& aliveFlag);

    ~TSnapshotHandle();

    TSnapshotHandle(TSnapshotHandle&& other) noexcept;
    TSnapshotHandle& operator=(TSnapshotHandle&& other) noexcept;

    TSnapshotHandle(const TSnapshotHandle&) = delete;
    TSnapshotHandle& operator=(const TSnapshotHandle&) = delete;

    void Reset();

private:
    std::shared_ptr<std::atomic<bool>> Alive_;
};

} // namespace NKikimr::NKqp