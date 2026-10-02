#include "fake_mmap.h"

#include <util/generic/singleton.h>
#include <util/system/yassert.h>

#include <yql/essentials/utils/exception_utils.h>

namespace NKikimr {

TFakeMmap& TFakeMmap::GetInstance() {
    return *Singleton<TFakeMmap>();
}

void* TFakeMmap::Mmap(size_t size) {
    Y_DEBUG_ABORT_UNLESS(OnMmap, "mmap function must be provided");
    return OnMmap(size);
}

int TFakeMmap::Munmap(void* addr, size_t size) noexcept {
    return NYql::WithAbortOnException([&] {
        if (OnMunmap) {
            OnMunmap(addr, size);
        }
        return 0;
    }, "TFakeMmap::Munmap");
}

} // namespace NKikimr
