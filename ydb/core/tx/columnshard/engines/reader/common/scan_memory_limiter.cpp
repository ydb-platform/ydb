#include "scan_memory_limiter.h"

#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

namespace NKikimr::NOlap::NReader {

std::shared_ptr<NGroupedMemoryManager::TStageFeatures> BuildScanStageFeatures(
    EScanGroupedMemoryLimiterOperator operatorType, const TString& name, const ui64 limit) {
    switch (operatorType) {
        case EScanGroupedMemoryLimiterOperator::Deduplication:
            return NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::BuildStageFeatures(name, limit);
        case EScanGroupedMemoryLimiterOperator::Scan:
            return NGroupedMemoryManager::TScanMemoryLimiterOperator::BuildStageFeatures(name, limit);
    }
}

std::shared_ptr<NGroupedMemoryManager::TProcessGuard> BuildScanProcessGuard(EScanGroupedMemoryLimiterOperator operatorType,
    const ui64 externalProcessId, const std::vector<std::shared_ptr<NGroupedMemoryManager::TStageFeatures>>& stages) {
    switch (operatorType) {
        case EScanGroupedMemoryLimiterOperator::Deduplication:
            return NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::BuildProcessGuard(externalProcessId, stages);
        case EScanGroupedMemoryLimiterOperator::Scan:
            return NGroupedMemoryManager::TScanMemoryLimiterOperator::BuildProcessGuard(externalProcessId, stages);
    }
}

bool SendScanToAllocation(EScanGroupedMemoryLimiterOperator operatorType, const ui64 processId, const ui64 scopeId, const ui64 groupId,
    const std::vector<std::shared_ptr<NGroupedMemoryManager::IAllocation>>& tasks, const std::optional<ui32>& stageIdx) {
    switch (operatorType) {
        case EScanGroupedMemoryLimiterOperator::Deduplication:
            return NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::SendToAllocation(processId, scopeId, groupId, tasks, stageIdx);
        case EScanGroupedMemoryLimiterOperator::Scan:
            return NGroupedMemoryManager::TScanMemoryLimiterOperator::SendToAllocation(processId, scopeId, groupId, tasks, stageIdx);
    }
}

}   // namespace NKikimr::NOlap::NReader
