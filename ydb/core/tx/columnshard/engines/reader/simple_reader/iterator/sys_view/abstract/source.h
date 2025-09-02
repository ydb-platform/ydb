#pragma once
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/constructor.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NAbstract {

class TSourceData: public NReader::NSimple::IDataSource {
private:
    using TBase = NReader::NSimple::IDataSource;
    YDB_READONLY(ui64, TabletId, 0);
    const NCommon::TReplaceKeyAdapter Start;
    const NCommon::TReplaceKeyAdapter Finish;

    virtual TConclusion<bool> DoStartFetchImpl(const NArrow::NSSA::TProcessorContext& /*context*/,
        const std::vector<std::shared_ptr<NReader::NCommon::IKernelFetchLogic>>& /*fetchersExt*/) override {
        return false;
    }

    virtual bool NeedPortionData() const override {
        return false;
    }

    virtual bool DoStartFetchingAccessor(
        const std::shared_ptr<NCommon::IDataSource>& /*sourcePtr*/, const NReader::NCommon::TFetchingScriptCursor& /*step*/) override {
        return false;
    }

    virtual std::shared_ptr<arrow::Array> BuildArrayAccessor(const ui64 columnId, const ui32 recordsCount) const = 0;

    virtual void DoAssembleColumns(const std::shared_ptr<NReader::NCommon::TColumnsSet>& columns, const bool /*sequential*/) override {
        const ui32 recordsCount = GetRecordsCount();
        for (auto&& i : columns->GetColumnIds()) {
            if (i == (ui64)IIndexInfo::ESpecialColumn::PLAN_STEP || i == (ui64)IIndexInfo::ESpecialColumn::TX_ID ||
                i == (ui64)IIndexInfo::ESpecialColumn::WRITE_ID) {
                MutableStageData().MutableTable().AddVerified(i,
                    std::make_shared<NArrow::NAccessor::TTrivialArray>(
                        NArrow::TThreadSimpleArraysCache::GetConst(arrow::uint64(), std::make_shared<arrow::UInt64Scalar>(0), recordsCount)),
                    true);
            } else {
                MutableStageData().MutableTable().AddVerified(
                    i, std::make_shared<NArrow::NAccessor::TTrivialArray>(BuildArrayAccessor(i, recordsCount)), true);
            }
        }
    }

    virtual bool DoStartFetchingColumns(const std::shared_ptr<NReader::NCommon::IDataSource>& /*sourcePtr*/,
        const NReader::NCommon::TFetchingScriptCursor& /*step*/, const NReader::NCommon::TColumnsSetIds& /*columns*/) override {
        return false;
    }

    virtual TConclusion<std::shared_ptr<NArrow::NSSA::IFetchLogic>> DoStartFetchData(
        const NArrow::NSSA::TProcessorContext& /*context*/, const NArrow::NSSA::IDataSource::TDataAddress& /*addr*/) override {
        return std::shared_ptr<NArrow::NSSA::IFetchLogic>();
    }

    virtual TConclusion<std::shared_ptr<NArrow::NSSA::IFetchLogic>> DoStartFetchHeader(
        const NArrow::NSSA::TProcessorContext& /*context*/, const NArrow::NSSA::IDataSource::TFetchHeaderContext& /*fetchContext*/) override {
        return std::shared_ptr<NArrow::NSSA::IFetchLogic>();
    }

    virtual NArrow::TSimpleRow GetStartPKRecordBatch() const override {
        if (GetContext()->GetReadMetadata()->IsDescSorted()) {
            return Finish.GetValue();
        } else {
            return Start.GetValue();
        }
    }

    virtual NArrow::TSimpleRow GetMinPK() const override {
        return Start.GetValue();
    }

    virtual NArrow::TSimpleRow GetMaxPK() const override {
        return Finish.GetValue();
    }

    virtual THashMap<TChunkAddress, TString> DecodeBlobAddresses(
        NBlobOperations::NRead::TCompositeReadBlobs&& /*blobsOriginal*/) const override {
        AFL_VERIFY(false);
        return THashMap<TChunkAddress, TString>();
    }

    virtual ui64 GetColumnsVolume(const std::set<ui32>& /*columnIds*/, const NReader::NCommon::EMemType /*type*/) const override {
        return 0;
    }

    virtual ui64 PredictAccessorsSize(const std::set<ui32>& /*entityIds*/) const override {
        return 0;
    }

    virtual ui64 GetColumnRawBytes(const std::set<ui32>& /*columnsIds*/) const override {
        return 0;
    }

    virtual ui64 GetColumnBlobBytes(const std::set<ui32>& /*columnsIds*/) const override {
        return 0;
    }

    virtual ui64 GetIndexRawBytes(const std::set<ui32>& /*indexIds*/) const override {
        return 0;
    }

    virtual NColumnShard::TInternalPathId GetPathId() const override {
        AFL_VERIFY(false);
        return NColumnShard::TInternalPathId::FromRawValue(0);
    }

    virtual TConclusion<std::vector<std::shared_ptr<NArrow::NSSA::IFetchLogic>>> DoStartFetchIndex(
        const NArrow::NSSA::TProcessorContext& /*context*/, const TFetchIndexContext& /*fetchContext*/) override {
        return std::vector<std::shared_ptr<NArrow::NSSA::IFetchLogic>>();
    }

    virtual TConclusion<NArrow::TColumnFilter> DoCheckIndex(const NArrow::NSSA::TProcessorContext& /*context*/,
        const TCheckIndexContext& /*fetchContext*/, const std::shared_ptr<arrow::Scalar>& /*value*/) override {
        return NArrow::TColumnFilter::BuildAllowFilter();
    }

    virtual NJson::TJsonValue DoDebugJson() const override {
        return NJson::JSON_NULL;
    }

    virtual void InitUsedRawBytes() override {
        AFL_VERIFY(!UsedRawBytes);
        UsedRawBytes = 0;
    }

    virtual void DoAbort() override {
        return;
    }

    virtual bool HasIndexes(const std::set<ui32>& /*indexIds*/) const override {
        return false;
    }

    virtual bool DoAddTxConflict() override {
        AFL_VERIFY(false);
        return false;
    }

    virtual TConclusion<NArrow::TColumnFilter> DoCheckHeader(
        const NArrow::NSSA::TProcessorContext& /*context*/, const TCheckHeaderContext& /*fetchContext*/) override {
        return TConclusionStatus::Fail("incorrect method usage DoCheckHeader");
    }

protected:
    virtual void DoAssembleAccessor(const NArrow::NSSA::TProcessorContext& context, const ui32 columnId, const TString& subColumnName) override {
        const ui32 recordsCount = GetRecordsCount();
        AFL_VERIFY(!subColumnName);
        if (columnId == (ui64)IIndexInfo::ESpecialColumn::PLAN_STEP || columnId == (ui64)IIndexInfo::ESpecialColumn::TX_ID ||
            columnId == (ui64)IIndexInfo::ESpecialColumn::WRITE_ID) {
            context.MutableResources().AddVerified(columnId,
                std::make_shared<NArrow::NAccessor::TTrivialArray>(
                    NArrow::TThreadSimpleArraysCache::GetConst(arrow::uint64(), std::make_shared<arrow::UInt64Scalar>(0), recordsCount)),
                true);
        } else {
            context.MutableResources().AddVerified(
                columnId, std::make_shared<NArrow::NAccessor::TTrivialArray>(BuildArrayAccessor(columnId, recordsCount)), true);
        }
    }

public:
    static bool CheckTypeCast(const EType type) {
        return type == NCommon::IDataSource::EType::SimpleSysInfo;
    }

    TSourceData(const ui32 sourceId, const ui32 sourceIdx, const ui64 tabletId, const NOlap::TSnapshot& minSnapshot,
        const NOlap::TSnapshot& maxSnapshot, NArrow::TSimpleRow&& start, NArrow::TSimpleRow&& finish, const std::optional<ui32> recordsCount,
        const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context)
        : TBase(EType::SimpleSysInfo, sourceId, sourceIdx, context, minSnapshot, maxSnapshot, recordsCount, std::nullopt, false)
        , TabletId(tabletId)
        , Start(context->GetReadMetadata()->IsDescSorted() ? std::move(finish) : std::move(start), context->GetReadMetadata()->IsDescSorted())
        , Finish(context->GetReadMetadata()->IsDescSorted() ? std::move(start) : std::move(finish), context->GetReadMetadata()->IsDescSorted()) {
    }
};

class TTabletSourceData: public TSourceData {
private:
    using TBase = TSourceData;

public:
    virtual NColumnShard::TInternalPathId GetPathId() const override {
        AFL_VERIFY(false);
        return NColumnShard::TInternalPathId::FromRawValue(0);
    }

    TTabletSourceData(const ui32 sourceId, const ui32 sourceIdx, const ui64 tabletId, NArrow::TSimpleRow&& start, NArrow::TSimpleRow&& finish,
        const std::optional<ui32> recordsCount, const NOlap::TSnapshot& minSnapshot, const NOlap::TSnapshot& maxSnapshot,
        const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context)
        : TBase(sourceId, sourceIdx, tabletId, minSnapshot, maxSnapshot, std::move(start), std::move(finish), recordsCount, context) {
    }
};

class TPathSourceData: public TSourceData {
private:
    using TBase = TSourceData;
    YDB_READONLY_DEF(NColumnShard::TUnifiedPathId, UnifiedPathId);

public:
    virtual NColumnShard::TInternalPathId GetPathId() const override {
        return GetUnifiedPathId().GetInternalPathId();
    }

    TPathSourceData(const ui32 sourceId, const ui32 sourceIdx, const NColumnShard::TUnifiedPathId& pathId, const ui64 tabletId,
        NArrow::TSimpleRow&& start, NArrow::TSimpleRow&& finish, const std::optional<ui32> recordsCount, const NOlap::TSnapshot& minSnapshot,
        const NOlap::TSnapshot& maxSnapshot, const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context)
        : TBase(sourceId, sourceIdx, tabletId, minSnapshot, maxSnapshot, std::move(start), std::move(finish), recordsCount, context)
        , UnifiedPathId(pathId) {
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NAbstract
