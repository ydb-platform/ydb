#pragma once
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/constructor.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/sub_columns_fetching.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view/abstract/source.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks {

class TSourceData: public NAbstract::TPathSourceData {
private:
    using TBase = NAbstract::TPathSourceData;
    YDB_READONLY_DEF(TPortionInfo::TConstPtr, Portion);
    ISnapshotSchema::TPtr Schema;
    std::shared_ptr<NArrow::NAccessor::TAccessorsCollection> OriginalData;

    virtual TString GetColumnStorageId(const ui32 columnId) const override {
        return GetStageData().GetPortionAccessor().GetPortionInfo().GetColumnStorageId(columnId, Schema->GetIndexInfo());
    }

    virtual ui64 GetColumnRawBytes(const std::set<ui32>& /*columnsIds*/) const override {
        return 0;
    }

    virtual ui64 GetColumnBlobBytes(const std::set<ui32>& /*columnsIds*/) const override {
        return 0;
    }

    virtual TBlobRange RestoreBlobRange(const TBlobRangeLink16& rangeLink) const override {
        return GetStageData().GetPortionAccessor().RestoreBlobRange(rangeLink);
    }

    virtual bool DoStartFetchingAccessor(
        const std::shared_ptr<IDataSource>& sourcePtr, const NReader::NCommon::TFetchingScriptCursor& step) override;

    virtual std::shared_ptr<arrow::Array> BuildArrayAccessor(const ui64 columnId, const ui32 recordsCount) const override;

    virtual void InitUsedRawBytes() override {
        AFL_VERIFY(!UsedRawBytes);
        UsedRawBytes = GetStageData().GetPortionAccessor().GetColumnRawBytes(GetContext()->GetAllUsageColumns()->GetColumnIds(), false);
        InitRecordsCount(GetRecordsCount());
    }

    virtual bool NeedPortionData() const override {
        return true;
    }

    virtual ui32 GetRecordsCountVirtual() const override {
        AFL_VERIFY(HasStageData())("tablet_id", GetTabletId())("source_id", GetSourceId());
        return GetStageData().GetPortionAccessor().GetRecordsVerified().size() + GetStageData().GetPortionAccessor().GetIndexesVerified().size();
    }

    virtual void DoAssembleAccessor(const NArrow::NSSA::TProcessorContext& context, const ui32 columnId, const TString& subColumnName) override;

    virtual TConclusion<bool> DoStartFetchImpl(
        const NArrow::NSSA::TProcessorContext& context, const std::vector<std::shared_ptr<NCommon::IKernelFetchLogic>>& fetchersExt) override;

    virtual TConclusion<std::shared_ptr<NArrow::NSSA::IFetchLogic>> DoStartFetchData(
        const NArrow::NSSA::TProcessorContext& context, const NArrow::NSSA::IDataSource::TDataAddress& addr) override;

public:
    TSourceData(const ui32 sourceId, const ui32 sourceIdx, const NColumnShard::TUnifiedPathId& pathId, const ui64 tabletId,
        TPortionInfo::TConstPtr&& portion, NArrow::TSimpleRow&& start, NArrow::TSimpleRow&& finish,
        const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context, ISnapshotSchema::TPtr&& schema)
        : TBase(sourceId, sourceIdx, pathId, tabletId, std::move(start), std::move(finish), std::nullopt, portion->RecordSnapshotMin(),
              portion->RecordSnapshotMin(), context)
        , Portion(std::move(portion))
        , Schema(std::move(schema)) {
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks
