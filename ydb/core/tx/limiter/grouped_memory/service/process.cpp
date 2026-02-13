#include "process.h"

#include <util/string/builder.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

TString TProcessMemoryUsage::DebugString() const {
    return TStringBuilder() << "{mem:" << MemoryUsage << ";process_id:" << InternalProcessId << "}";
}

TString TProcessMemoryScope::DebugString() const {
    TStringBuilder sb;
    sb << "TProcessMemoryScope{" << Endl
       << "  ExternalProcessId=" << ExternalProcessId << Endl
       << "  ExternalScopeId=" << ExternalScopeId << Endl
       << "  Links=" << Links << Endl
       << "  OwnerActorId=" << OwnerActorId.ToString() << Endl
       << "  WaitAllocations=" << WaitAllocations.DebugString() << Endl
       << "  AllocationInfoCount=" << AllocationInfo.size() << Endl
       << "  AllocationInfo=[" << Endl;
    
    bool first = true;
    for (const auto& [id, info] : AllocationInfo) {
        if (!first) {
            sb << "," << Endl;
        }
        first = false;
        sb << "    {Id=" << id << ";Info=" << info->DebugString() << "}";
    }
    
    sb << Endl << "  ]" << Endl
       << "  GroupIds={" << Endl
       << "    Size=" << GroupIds.GetSize() << Endl
       << "    MinExternalId=" << (GroupIds.GetMinExternalIdOptional().has_value()
                                  ? ToString(GroupIds.GetMinExternalIdOptional().value())
                                  : "null") << Endl
       << "    ExternalIds=[" << Endl;
    
    first = true;
    for (const auto& id : GroupIds.GetExternalIds()) {
        if (!first) {
            sb << "," << Endl;
        }
        first = false;
        sb << "      " << id;
    }
    
    sb << Endl << "    ]" << Endl << "  }" << Endl << "}";
    return sb;
}

TString TProcessMemory::DebugString() const {
    TStringBuilder builder;
    builder << "{" << Endl
        << "  ExternalProcessId: " << ExternalProcessId << Endl
        << "  InternalProcessId: " << InternalProcessId << Endl
        << "  OwnerActorId: " << OwnerActorId.ToString() << Endl
        << "  PriorityProcessFlag: " << PriorityProcessFlag << Endl
        << "  MemoryUsage: " << MemoryUsage << Endl
        << "  LinksCount: " << LinksCount << Endl
        << "  DefaultStage: " << DefaultStage->DebugString() << Endl;
    for (size_t i = 0; i < Stages.size(); ++i) {
        builder << "  Stages " << i << ": " << Stages[i]->DebugString() << Endl;
    }
    for (const auto& [id, scope]: AllocationScopes) {
        builder << "  AllocationScopes " << id << ": " << scope->DebugString() << Endl;
    }
    for (const auto& id: WaitingScopes) {
        builder << "  WaitingScopes " << id << Endl;
    }
    return builder << "}";
}

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
