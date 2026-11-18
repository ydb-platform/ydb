#pragma once

#include <ydb/core/tx/columnshard/column_fetching/cache_policy.h>
#include <ydb/core/tx/columnshard/data_accessor/cache_policy/policy.h>
#include <ydb/core/tx/columnshard/stats/scan/observation.h>
#include <ydb/core/tx/general_cache/usage/abstract.h>

namespace NKikimr::NOlap::NStats::NScan {

std::shared_ptr<::NKikimr::NGeneralCache::NPublic::IRequestObserver<NGeneralCache::TPortionsMetadataCachePolicy>> MakeMetadataCacheObserver(
    std::unique_ptr<TLocalStats> local, const TOperationNode* parent = nullptr,
    std::function<bool(const NColumnShardStats::TDataSource&, std::optional<ui64>)> attributeRead = {});

std::shared_ptr<::NKikimr::NGeneralCache::NPublic::IRequestObserver<NGeneralCache::TColumnDataCachePolicy>> MakeColumnCacheObserver(
    std::unique_ptr<TLocalStats> local);

}   // namespace NKikimr::NOlap::NStats::NScan
