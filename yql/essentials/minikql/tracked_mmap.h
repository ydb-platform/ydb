#pragma once

#include <util/generic/yexception.h>
#include <util/system/types.h>

#include <atomic>
#include <cstddef>
#include <expected>

namespace NKikimr {

template <typename TProvider>
class TTrackedMmap {
public:
    TTrackedMmap();

    static TTrackedMmap& GetInstance();

    std::expected<void*, TSystemError> Mmap(size_t size);
    std::expected<void, TSystemError> Munmap(void* addr, size_t size, bool frozen);
    std::expected<void, TSystemError> Freeze(void* addr, size_t size);
    std::expected<void, TSystemError> Unfreeze(void* addr, size_t size);

    i64 GetTotalCommittedBytes() const noexcept;

private:
    void AddCommittedBytes(size_t size) noexcept;
    void SubtractCommittedBytes(size_t size) noexcept;

    TProvider& Provider_;
    std::atomic<i64> TotalCommittedBytes_{0};
};

} // namespace NKikimr
