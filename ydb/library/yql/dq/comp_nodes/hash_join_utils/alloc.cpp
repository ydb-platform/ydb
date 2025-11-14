#include "alloc.h"
#include <yql/essentials/minikql/defs.h>
#include <util/string/printf.h>
namespace NKikimr::NMiniKQL{


int MemoryUsagePercent(int totalBytes) {
    return 100*totalBytes / TlsAllocState->GetLimit();
}

int FreeMemory() {
    MKQL_ENSURE(TlsAllocState->GetLimit() >= TlsAllocState->GetUsed(), Sprintf("sanity check, limit: %i, alloc: %i", TlsAllocState->GetLimit(), TlsAllocState->GetUsed()));
    return TlsAllocState->GetLimit() - TlsAllocState->GetUsed(); 
}

bool AllocateWithSizeMayThrow(i64 size) {
    return FreeMemory() > size;
}

std::optional<int> GetMemoryUsageIfReachedLimit() {
    if (!TlsAllocState->GetMaximumLimitValueReached()) {
        return std::nullopt;
    }
    return std::make_optional<int>(MemoryUsagePercent(TlsAllocState->GetUsed()));
}

bool MemoryPercentIsFree(int freePercent) {

    std::optional<int> usedPercent = GetMemoryUsageIfReachedLimit();
    return !usedPercent.has_value() || (freePercent + *usedPercent < 100);
}
}