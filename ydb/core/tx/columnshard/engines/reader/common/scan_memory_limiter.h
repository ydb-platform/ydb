#pragma once

#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>

namespace NKikimr::NOlap::NReader {

enum class EScanGroupedMemoryLimiterOperator {
    Scan,
    Deduplication,
};

std::shared_ptr<NGroupedMemoryManager::TStageFeatures> BuildScanStageFeatures(
    EScanGroupedMemoryLimiterOperator operatorType, const TString& name, const ui64 limit);

std::shared_ptr<NGroupedMemoryManager::TProcessGuard> BuildScanProcessGuard(EScanGroupedMemoryLimiterOperator operatorType,
    const ui64 externalProcessId, const std::vector<std::shared_ptr<NGroupedMemoryManager::TStageFeatures>>& stages);

bool SendScanToAllocation(EScanGroupedMemoryLimiterOperator operatorType, const ui64 processId, const ui64 scopeId, const ui64 groupId,
    const std::vector<std::shared_ptr<NGroupedMemoryManager::IAllocation>>& tasks, const std::optional<ui32>& stageIdx);

}   // namespace NKikimr::NOlap::NReader
