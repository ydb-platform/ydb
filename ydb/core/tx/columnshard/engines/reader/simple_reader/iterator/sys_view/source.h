#pragma once
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/constructor.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NOlap::NReader::NSimple {

class TSysViewAbstractInfo: public NReader::NSimple::IDataSource {
private:
    using TBase = NReader::NSimple::IDataSource;
    YDB_READONLY(ui64, TabletId, 0);

    virtual TConclusion<bool> DoStartFetchImpl(const NArrow::NSSA::TProcessorContext& /*context*/,
        const std::vector<std::shared_ptr<NReader::NCommon::IKernelFetchLogic>>& /*fetchersExt*/) override {
        return false;
    }

    virtual bool DoStartFetchingAccessor(
        const std::shared_ptr<IDataSource>& /*sourcePtr*/, const NReader::NCommon::TFetchingScriptCursor& /*step*/) override {
        return false;
    }

    virtual std::shared_ptr<arrow::Array> BuildArrayAccessor(const ui64 columnId, const ui32 recordsCount) const = 0;

    virtual void DoAssembleColumns(const std::shared_ptr<NReader::NCommon::TColumnsSet>& columns, const bool /*sequential*/) override {
        const ui32 recordsCount = GetRecordsCount();
        for (auto&& i : columns->GetColumnIds()) {
            if (i == (ui64)IIndexInfo::ESpecialColumn::PLAN_STEP || i == (ui64)IIndexInfo::ESpecialColumn::TX_ID ||
                i == (ui64)IIndexInfo::ESpecialColumn::WRITE_ID) {
                MutableStageData().GetTable()->AddVerified(i,
                    std::make_shared<NArrow::NAccessor::TTrivialArray>(
                        NArrow::TThreadSimpleArraysCache::GetConst(arrow::uint64(), std::make_shared<arrow::UInt64Scalar>(0), recordsCount)),
                    true);
            } else {
                MutableStageData().GetTable()->AddVerified(
                    i, std::make_shared<NArrow::NAccessor::TTrivialArray>(BuildArrayAccessor(i, recordsCount)), true);
            }
        }
    }

    virtual bool DoStartFetchingColumns(const std::shared_ptr<NReader::NCommon::IDataSource>& /*sourcePtr*/,
        const NReader::NCommon::TFetchingScriptCursor& /*step*/, const NReader::NCommon::TColumnsSetIds& /*columns*/) override {
        return false;
    }

    virtual void DoAssembleAccessor(
        const NArrow::NSSA::TProcessorContext& context, const ui32 columnId, const TString& /*subColumnName*/) override {
        const ui32 recordsCount = GetRecordsCount();
        if (columnId == (ui64)IIndexInfo::ESpecialColumn::PLAN_STEP || columnId == (ui64)IIndexInfo::ESpecialColumn::TX_ID ||
            columnId == (ui64)IIndexInfo::ESpecialColumn::WRITE_ID) {
            context.GetResources()->AddVerified(columnId,
                std::make_shared<NArrow::NAccessor::TTrivialArray>(
                    NArrow::TThreadSimpleArraysCache::GetConst(arrow::uint64(), std::make_shared<arrow::UInt64Scalar>(0), recordsCount)),
                true);
        } else {
            context.GetResources()->AddVerified(
                columnId, std::make_shared<NArrow::NAccessor::TTrivialArray>(BuildArrayAccessor(columnId, recordsCount)), true);
        }
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
            return GetFinish().GetValue();
        } else {
            return GetStart().GetValue();
        }
    }

    virtual NArrow::TSimpleRow GetMinPK() const override {
        return GetStart().GetValue();
    }

    virtual NArrow::TSimpleRow GetMaxPK() const override {
        return GetFinish().GetValue();
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

    virtual ui32 GetRecordsCountVirtual() const override {
        if (HasStageData()) {
            return GetStageData().GetPortionAccessor().GetRecordsVerified().size() +
                   GetStageData().GetPortionAccessor().GetIndexesVerified().size();
        } else {
            return GetStageResult().GetBatch()->GetRecordsCount();
        }
    }

public:
    TSysViewAbstractInfo(const ui32 sourceId, const ui32 sourceIdx, const ui64 tabletId, const NOlap::TSnapshot& minSnapshot,
        const NOlap::TSnapshot& maxSnapshot, NArrow::TSimpleRow&& start, NArrow::TSimpleRow&& finish,
        const std::shared_ptr<NReader::NSimple::TSpecialReadContext>& context)
        : TBase(sourceId, sourceIdx, context, std::move(start), std::move(finish), minSnapshot, maxSnapshot, std::nullopt, std::nullopt, false)
        , TabletId(tabletId) {
    }
};

class TSysViewAbstractPortionInfo: public TSysViewAbstractInfo {
private:
    using TBase = TSysViewAbstractInfo;
    YDB_READONLY_DEF(NColumnShard::TUnifiedPathId, UnifiedPathId);
    YDB_READONLY_DEF(TPortionInfo::TConstPtr, Portion);

public:
    virtual NColumnShard::TInternalPathId GetPathId() const override {
        return GetUnifiedPathId().GetInternalPathId();
    }

    TSysViewAbstractPortionInfo(const ui32 sourceId, const ui32 sourceIdx, const NColumnShard::TUnifiedPathId& pathId, const ui64 tabletId,
        const TPortionInfo::TConstPtr& portion, NArrow::TSimpleRow&& start, NArrow::TSimpleRow&& finish,
        const std::shared_ptr<NReader::NSimple::TSpecialReadContext>& context)
        : TBase(sourceId, sourceIdx, tabletId, portion->RecordSnapshotMin(), portion->RecordSnapshotMin(), std::move(start), std::move(finish),
              context)
        , UnifiedPathId(pathId)
        , Portion(portion) {
    }
};

class TSysViewPortionChunksInfo: public TSysViewAbstractPortionInfo {
private:
    using TBase = TSysViewAbstractPortionInfo;

    virtual bool DoStartFetchingAccessor(
        const std::shared_ptr<IDataSource>& sourcePtr, const NReader::NCommon::TFetchingScriptCursor& step) override;

    virtual std::shared_ptr<arrow::Array> BuildArrayAccessor(const ui64 columnId, const ui32 recordsCount) const override {
        if (columnId == 1) {
            return NArrow::TStatusValidator::GetValid(
                arrow::MakeArrayFromScalar(arrow::UInt64Scalar(GetUnifiedPathId().GetSchemeShardLocalPathId().GetRawValue()), recordsCount));
        }
        if (columnId == 2) {
            return NArrow::TStatusValidator::GetValid(
                arrow::MakeArrayFromScalar(arrow::StringScalar(::ToString(GetPortion()->GetProduced())), recordsCount));
        }
        if (columnId == 3) {
            return NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::UInt64Scalar(GetTabletId()), recordsCount));
        }
        if (columnId == 6) {
            return NArrow::TStatusValidator::GetValid(
                arrow::MakeArrayFromScalar(arrow::UInt64Scalar(GetPortion()->GetPortionId()), recordsCount));
        }
        if (columnId == 11) {
            auto builder = NArrow::MakeBuilder(arrow::uint64());
            for (auto&& i : GetStageData().GetPortionAccessor().GetRecordsVerified()) {
                NArrow::Append<arrow::UInt64Type>(*builder, i.GetBlobRange().GetOffset());
            }
            for (auto&& i : GetStageData().GetPortionAccessor().GetIndexesVerified()) {
                if (auto range = i.GetBlobRangeOptional()) {
                    NArrow::Append<arrow::UInt64Type>(*builder, range->GetOffset());
                } else {
                    NArrow::Append<arrow::UInt64Type>(*builder, 0);
                }
            }
            return NArrow::FinishBuilder(std::move(builder));
        }
        if (columnId == 4) {
            auto builder = NArrow::MakeBuilder(arrow::uint64());
            for (auto&& i : GetStageData().GetPortionAccessor().GetRecordsVerified()) {
                NArrow::Append<arrow::UInt64Type>(*builder, i.GetMeta().GetRecordsCount());
            }
            for (auto&& i : GetStageData().GetPortionAccessor().GetIndexesVerified()) {
                NArrow::Append<arrow::UInt64Type>(*builder, i.GetRecordsCount());
            }
            return NArrow::FinishBuilder(std::move(builder));
        }
        if (columnId == 12) {
            auto builder = NArrow::MakeBuilder(arrow::uint64());
            for (auto&& i : GetStageData().GetPortionAccessor().GetRecordsVerified()) {
                NArrow::Append<arrow::UInt64Type>(*builder, i.GetBlobRange().GetSize());
            }
            for (auto&& i : GetStageData().GetPortionAccessor().GetIndexesVerified()) {
                NArrow::Append<arrow::UInt64Type>(*builder, i.GetDataSize());
            }
            return NArrow::FinishBuilder(std::move(builder));
        }
        if (columnId == 13) {
            if (GetPortion()->IsRemovedFor(GetContext()->GetCommonContext()->GetReadMetadata()->GetRequestSnapshot())) {
                return NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::UInt8Scalar(0), recordsCount));
            } else {
                return NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::UInt8Scalar(1), recordsCount));
            }
        }
        AFL_VERIFY(false)("column_id", columnId);
        return nullptr;
    }

    virtual void InitUsedRawBytes() override {
        AFL_VERIFY(!UsedRawBytes);
        UsedRawBytes = GetStageData().GetPortionAccessor().GetColumnRawBytes(GetContext()->GetAllUsageColumns()->GetColumnIds(), false);
        MutableStageData().InitRecordsCount(GetRecordsCount());
    }

    virtual ui32 GetRecordsCountVirtual() const override {
        if (HasStageData()) {
            return GetStageData().GetPortionAccessor().GetRecordsVerified().size() +
                   GetStageData().GetPortionAccessor().GetIndexesVerified().size();
        } else {
            return GetStageResult().GetBatch()->GetRecordsCount();
        }
    }

public:
    TSysViewPortionChunksInfo(const ui32 sourceId, const ui32 sourceIdx, const NColumnShard::TUnifiedPathId& pathId, const ui64 tabletId,
        const TPortionInfo::TConstPtr& portion, NArrow::TSimpleRow&& start, NArrow::TSimpleRow&& finish,
        const std::shared_ptr<NReader::NSimple::TSpecialReadContext>& context)
        : TBase(sourceId, sourceIdx, pathId, tabletId, portion, std::move(start), std::move(finish), context) {
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
