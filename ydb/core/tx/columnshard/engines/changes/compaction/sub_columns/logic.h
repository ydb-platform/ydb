#pragma once
#include "iterator.h"

#include <ydb/core/formats/arrow/accessor/common/const.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/settings.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction/abstract/merger.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>

namespace NKikimr::NOlap::NCompaction {

class TSubColumnsMerger: public IColumnMerger {
private:
    static inline auto Registrator = TFactory::TRegistrator<TSubColumnsMerger>(NArrow::NAccessor::TGlobalConst::SubColumnsDataAccessorName);
    using TBase = IColumnMerger;
    using TDictStats = NArrow::NAccessor::NSubColumns::TDictStats;
    using TOthersData = NArrow::NAccessor::NSubColumns::TOthersData;
    using TColumnsData = NArrow::NAccessor::NSubColumns::TColumnsData;
    using TSubColumnsArray = NArrow::NAccessor::TSubColumnsArray;
    using TSettings = NArrow::NAccessor::NSubColumns::TSettings;
    using TRemapColumns = NKikimr::NOlap::NCompaction::NSubColumns::TRemapColumns;
    std::vector<std::shared_ptr<TSubColumnsArray>> Sources;
    std::optional<TDictStats> ResultColumnStats;
    TRemapColumns RemapKeyIndex;

    const TSettings& GetSettings() const;
    std::vector<NSubColumns::TChunksIterator> OrderedIterators;

    virtual void DoStart(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input, TMergingContext& mergeContext) override;
    virtual TColumnPortionResult DoExecute(const TChunkMergeContext& context, TMergingContext& mergeContext) override;

public:
    using TBase::TBase;
};

}   // namespace NKikimr::NOlap::NCompaction
