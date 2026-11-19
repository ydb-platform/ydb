#pragma once

#include <ydb/core/tx/columnshard/blobs_reader/task.h>
#include <ydb/core/tx/columnshard/stats/scan/observation.h>

namespace NKikimr::NOlap::NStats::NScan {

std::function<bool(const NColumnShardStats::TDataSource&, std::optional<ui64>)> MakeReadAttributionCallback(
    TCallHandle current, std::function<bool()> canAttributeRead);

std::shared_ptr<NBlobOperations::NRead::IReadObserver> MakeReadObserver(std::unique_ptr<TLocalStats> local,
    const TOperationNode* attributionParent = nullptr, TCallHandle requestingScope = {},
    std::function<bool(const NColumnShardStats::TDataSource&, std::optional<ui64>)> attributeRead = {});

}   // namespace NKikimr::NOlap::NStats::NScan
