#include "fake_mmap.h"

#include <util/generic/singleton.h>
#include <util/system/yassert.h>

#include <yql/essentials/utils/exception_utils.h>

namespace NKikimr {

TFakeMmap& TFakeMmap::GetInstance() {
    return *Singleton<TFakeMmap>();
}

std::expected<void*, TSystemError> TFakeMmap::Mmap(size_t size) {
    Y_DEBUG_ABORT_UNLESS(OnMmap, "mmap function must be provided");
    return OnMmap(size);
}

std::expected<void, TSystemError> TFakeMmap::Munmap(void* addr, size_t size) noexcept {
    return NYql::WithAbortOnException([&] {
        return OnMunmap ? OnMunmap(addr, size) : std::expected<void, TSystemError>{};
    }, "TFakeMmap::Munmap");
}

std::expected<void, TSystemError> TFakeMmap::Freeze(void* addr, size_t size) noexcept {
    return NYql::WithAbortOnException([&] {
        return OnFreeze ? OnFreeze(addr, size) : std::expected<void, TSystemError>{};
    }, "TFakeMmap::Freeze");
}

std::expected<void, TSystemError> TFakeMmap::Unfreeze(void* addr, size_t size) noexcept {
    return NYql::WithAbortOnException([&] {
        return OnUnfreeze ? OnUnfreeze(addr, size) : std::expected<void, TSystemError>{};
    }, "TFakeMmap::Unfreeze");
}

} // namespace NKikimr
