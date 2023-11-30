#include "tcmalloc.h"

#include <contrib/libs/tcmalloc/tcmalloc/malloc_extension.h>

namespace NProfiling {

static thread_local ui32 AllocationTag = 0;

static struct TInitTCMallocCallbacks {
    static void* CreateTag() {
        return reinterpret_cast<void*>(AllocationTag);
    }
    static void* CopyTag(void* tag) {
        return tag;
    }
    static void DestroyTag(void* tag) {
        Y_UNUSED(tag);
    }

    TInitTCMallocCallbacks() {
        tcmalloc::MallocExtension::SetSampleUserDataCallbacks(
            CreateTag, CopyTag, DestroyTag);
    }
} InitTCMallocCallbacks;

ui32 SetTCMallocThreadAllocTag(ui32 tag) {
    ui32 prev = AllocationTag;
    AllocationTag = tag;
    return prev;
}

}
