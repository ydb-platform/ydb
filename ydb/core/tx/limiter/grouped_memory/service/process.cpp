#include "process.h"

#include <util/string/builder.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

TString TProcessMemoryUsage::DebugString() const {
    return TStringBuilder() << "{mem:" << MemoryUsage << ";process_id:" << InternalProcessId << "}";
}

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
