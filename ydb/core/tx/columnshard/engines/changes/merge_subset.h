#pragma once
#include "abstract/abstract.h"

#include <ydb/core/formats/arrow/container/container.h>
#include <ydb/core/formats/arrow/filter/filter.h>
#include <ydb/core/tx/columnshard/engines/portions/read_with_blobs.h>
#include <ydb/core/tx/columnshard/engines/portions/write_with_blobs.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/filtered_scheme.h>
#include <ydb/core/tx/columnshard/engines/storage/granule/portions_index.h>

namespace NKikimr::NOlap::NCompaction {

class TPortionToMerge {
private:
    YDB_READONLY_DEF(std::shared_ptr<NArrow::TGeneralContainer>, Batch);
    YDB_READONLY_DEF(std::shared_ptr<NArrow::TColumnFilter>, Filter);

public:
    TPortionToMerge(const std::shared_ptr<NArrow::TGeneralContainer>& batch, const std::shared_ptr<NArrow::TColumnFilter>& filter)
        : Batch(batch)
        , Filter(filter)
    {
    }
};

class ISubsetToMerge {
private:
    virtual std::vector<TPortionToMerge> DoBuildPortionsToMerge(const TConstructionContext& context, const std::set<ui32>& seqDataColumnIds,
        const std::shared_ptr<TFilteredSnapshotSchema>& resultFiltered, const THashSet<ui64>& usedPortionIds,
        const bool useDeletionFilter) const = 0;

protected:
    const std::shared_ptr<TGranuleMeta> GranuleMeta;
    const std::shared_ptr<const NGranule::NPortionsIndex::TPortionsIndex::TPortions> PortionsIndexSnapshot;
    std::shared_ptr<NArrow::TColumnFilter> BuildPortionFilter(const std::optional<NKikimr::NOlap::TGranuleShardingInfo>& shardingActual,
        const std::shared_ptr<NArrow::TGeneralContainer>& batch, const TPortionInfo& pInfo, const THashSet<ui64>& portionsInUsage,
        const bool useDeletionFilter) const;

public:
    ISubsetToMerge(const std::shared_ptr<TGranuleMeta>& granule,
        const std::shared_ptr<const NGranule::NPortionsIndex::TPortionsIndex::TPortions>& portionsIndexSnapshot)
        : GranuleMeta(granule)
        , PortionsIndexSnapshot(portionsIndexSnapshot)
    {
        AFL_VERIFY(GranuleMeta);
        AFL_VERIFY(PortionsIndexSnapshot);
    }

    virtual ~ISubsetToMerge() = default;

    std::vector<TPortionToMerge> BuildPortionsToMerge(const TConstructionContext& context, const std::set<ui32>& seqDataColumnIds,
        const std::shared_ptr<TFilteredSnapshotSchema>& resultFiltered, const THashSet<ui64>& usedPortionIds,
        const bool useDeletionFilter) const {
        return DoBuildPortionsToMerge(context, seqDataColumnIds, resultFiltered, usedPortionIds, useDeletionFilter);
    }

    virtual ui64 GetColumnMaxChunkMemory() const = 0;
};

class TReadPortionToMerge: public ISubsetToMerge {
private:
    using TBase = ISubsetToMerge;
    TReadPortionInfoWithBlobs ReadPortion;

    virtual std::vector<TPortionToMerge> DoBuildPortionsToMerge(const TConstructionContext& context, const std::set<ui32>& seqDataColumnIds,
        const std::shared_ptr<TFilteredSnapshotSchema>& resultFiltered, const THashSet<ui64>& usedPortionIds,
        const bool useDeletionFilter) const override;

public:
    TReadPortionToMerge(TReadPortionInfoWithBlobs&& rPortion, const std::shared_ptr<TGranuleMeta>& granuleMeta,
        const std::shared_ptr<const NGranule::NPortionsIndex::TPortionsIndex::TPortions>& portionsIndexSnapshot)
        : TBase(granuleMeta, portionsIndexSnapshot)
        , ReadPortion(std::move(rPortion))
    {
    }

    virtual ui64 GetColumnMaxChunkMemory() const override {
        ui64 result = 0;
        for (auto&& i : ReadPortion.GetChunks()) {
            result = std::max<ui64>(result, i.second->GetRawBytesVerified());
        }
        return result;
    }
};

class TWritePortionsToMerge: public ISubsetToMerge {
private:
    using TBase = ISubsetToMerge;
    std::vector<TWritePortionInfoWithBlobsResult> WritePortions;

    virtual std::vector<TPortionToMerge> DoBuildPortionsToMerge(const TConstructionContext& context, const std::set<ui32>& seqDataColumnIds,
        const std::shared_ptr<TFilteredSnapshotSchema>& resultFiltered, const THashSet<ui64>& usedPortionIds,
        const bool useDeletionFilter) const override;

    virtual ui64 GetColumnMaxChunkMemory() const override;

public:
    TWritePortionsToMerge(std::vector<TWritePortionInfoWithBlobsResult>&& portions, const std::shared_ptr<TGranuleMeta>& granuleMeta,
        const std::shared_ptr<const NGranule::NPortionsIndex::TPortionsIndex::TPortions>& portionsIndexSnapshot);
};

}   // namespace NKikimr::NOlap::NCompaction
