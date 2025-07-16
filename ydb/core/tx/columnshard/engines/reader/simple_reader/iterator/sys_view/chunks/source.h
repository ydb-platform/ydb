#pragma once
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/constructor.h>
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

    virtual void DoAssembleAccessor(const NArrow::NSSA::TProcessorContext& context, const ui32 columnId, const TString& subColumnName) override {
        if (columnId == 16) {
            auto source = context.GetDataSourceVerifiedAs<NCommon::IDataSource>();
            OriginalData = std::make_shared<NArrow::NAccessor::TAccessorsCollection>();
            for (auto&& i : GetStageData().GetPortionAccessor().GetRecordsVerified()) {
                NCommon::TFetchingResultContext fetchContext(*OriginalData, *GetStageData().GetIndexes(), source);
                if (auto fetcher = MutableStageData().ExtractFetcherOptional(i.GetEntityId())) {
                    fetcher->OnDataCollected(fetchContext);
                }
            }
        }
        TBase::DoAssembleAccessor(context, columnId, subColumnName);
    }

    virtual TConclusion<std::shared_ptr<NArrow::NSSA::IFetchLogic>> DoStartFetchData(
        const NArrow::NSSA::TProcessorContext& context, const NArrow::NSSA::IDataSource::TDataAddress& addr) override {
        if (addr.GetColumnId() == 16) {
            auto source = context.GetDataSourceVerifiedAs<NCommon::IDataSource>();

            for (auto&& i : GetStageData().GetPortionAccessor().GetRecordsVerified()) {
                if (Schema->GetColumnLoaderVerified(i.GetEntityId())->GetAccessorConstructor()->GetType() ==
                    NArrow::NAccessor::IChunkedArray::EType::SubColumnsArray) {
                    return std::make_shared<NCommon::TSubColumnsFetchLogic>(i.GetEntityId(), Schema,
                        GetContext()->GetCommonContext()->GetStoragesManager(),
                        GetStageData().GetPortionAccessor().GetPortionInfo().GetRecordsCount(), {});
                }
            }
        }
        return std::shared_ptr<NArrow::NSSA::IFetchLogic>();
    }

public:
    TSourceData(const ui32 sourceId, const ui32 sourceIdx, const NColumnShard::TUnifiedPathId& pathId, const ui64 tabletId,
        const TPortionInfo::TConstPtr& portion, NArrow::TSimpleRow&& start, NArrow::TSimpleRow&& finish,
        const std::shared_ptr<NReader::NSimple::TSpecialReadContext>& context, ISnapshotSchema::TPtr&& schema)
        : TBase(sourceId, sourceIdx, pathId, tabletId, std::move(start), std::move(finish), std::nullopt, portion->RecordSnapshotMin(),
              portion->RecordSnapshotMin(), context)
        , Portion(portion)
        , Schema(schema) {
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks
