#include "tracked_mmap.h"

#include "fake_mmap.h"
#include "system_mmap.h"

#include <util/generic/singleton.h>
#include <util/system/yassert.h>

#include <utility>

namespace NKikimr {

template <typename TProvider>
TTrackedMmap<TProvider>::TTrackedMmap()
    : Provider_(TProvider::GetInstance())
{
}

template <typename TProvider>
TTrackedMmap<TProvider>& TTrackedMmap<TProvider>::GetInstance() {
    return *Singleton<TTrackedMmap<TProvider>>();
}

template <typename TProvider>
std::expected<void*, TSystemError> TTrackedMmap<TProvider>::Mmap(size_t size) {
    auto result = Provider_.Mmap(size);
    if (result) {
        AddCommittedBytes(size);
    }
    return result;
}

template <typename TProvider>
std::expected<void, TSystemError> TTrackedMmap<TProvider>::Munmap(void* addr, size_t size, bool frozen) {
    auto result = Provider_.Munmap(addr, size);
    if (result && !frozen) {
        SubtractCommittedBytes(size);
    }
    return result;
}

template <typename TProvider>
std::expected<void, TSystemError> TTrackedMmap<TProvider>::Freeze(void* addr, size_t size) {
    auto result = Provider_.Freeze(addr, size);
    if (result) {
        SubtractCommittedBytes(size);
    }
    return result;
}

template <typename TProvider>
std::expected<void, TSystemError> TTrackedMmap<TProvider>::Unfreeze(void* addr, size_t size) {
    auto result = Provider_.Unfreeze(addr, size);
    if (result) {
        AddCommittedBytes(size);
    }
    return result;
}

template <typename TProvider>
i64 TTrackedMmap<TProvider>::GetTotalCommittedBytes() const noexcept {
    return TotalCommittedBytes_.load();
}

template <typename TProvider>
void TTrackedMmap<TProvider>::AddCommittedBytes(size_t size) noexcept {
    TotalCommittedBytes_.fetch_add(size);
}

template <typename TProvider>
void TTrackedMmap<TProvider>::SubtractCommittedBytes(size_t size) noexcept {
    const i64 previousCommittedBytes = TotalCommittedBytes_.fetch_sub(size);
    Y_DEBUG_ABORT_UNLESS(std::cmp_greater_equal(previousCommittedBytes, size));
}

template class TTrackedMmap<TSystemMmap>;
template class TTrackedMmap<TFakeMmap>;

} // namespace NKikimr
