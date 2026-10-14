#include "ttl.h"

#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/read_with_blobs.h>

#include <ydb/library/formats/arrow/replace_key.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/util/bit_util.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_ACTUALIZATION

namespace NKikimr::NOlap {

ui64 TTTLColumnEngineChanges::TMemoryPredictor::AddPortion(
    const TPortionInfo::TConstPtr& portionInfo, const TPortionEvictionFeatures& features, const TPortionDataAccessor* accessor) {
    SumBlobsMemory += portionInfo->GetTotalBlobBytes();
    ui64 workingMemory = portionInfo->GetTotalRawBytes();
    if (features.GetRebuildDecimalPrimaryKey()) {
        constexpr ui64 ArrayMetadataEstimateBytes = 256;
        constexpr ui64 WorkingArrayCopies = 4;
        constexpr ui64 OutputBufferCopies = 2;
        constexpr ui64 PermutationBufferCopies = 2;
        const ui64 rows = portionInfo->GetRecordsCount();
        // Stored RawBytes omits repeated defaults in sparse/absent columns.
        ui64 denseMemory = portionInfo->GetTotalRawBytes();
        THashSet<ui32> dictionaryColumns;
        for (const auto columnId : features.GetTargetScheme()->GetColumnIds()) {
            const auto loader = features.GetTargetScheme()->GetColumnLoaderVerified(columnId);
            const auto& type = loader->GetField()->type();
            denseMemory += arrow::BitUtil::BytesForBits(rows) + ArrayMetadataEstimateBytes;
            if (const auto fixed = std::dynamic_pointer_cast<arrow::FixedWidthType>(type)) {
                denseMemory += arrow::BitUtil::BytesForBits(rows * fixed->bit_width());
            } else {
                if (loader->GetAccessorConstructor()->GetType() == NArrow::NAccessor::IChunkedArray::EType::Dictionary) {
                    dictionaryColumns.emplace(columnId);
                }
                denseMemory += (rows + 1) * sizeof(ui64);   // offsets, including large binary
                if (const auto value = std::dynamic_pointer_cast<arrow::BaseBinaryScalar>(loader->GetDefaultValue()); value && value->is_valid) {
                    denseMemory += rows * value->value->size();
                }
            }
        }
        if (!dictionaryColumns.empty()) {
            if (accessor) {
                for (const auto& chunk : accessor->GetRecordsVerified()) {
                    if (dictionaryColumns.contains(chunk.GetColumnId())) {
                        const ui64 chunkRows = chunk.GetMeta().GetRecordsCount();
                        const ui64 rawBytes = chunk.GetMeta().GetRawBytes();
                        // Positions use at least one byte per row; the rest bounds a dictionary value.
                        AFL_VERIFY(rawBytes >= chunkRows);
                        denseMemory += chunkRows * (rawBytes - chunkRows);
                    }
                }
            } else {
                // Task batching precedes accessor loading, so only portion totals are available.
                denseMemory += rows * portionInfo->GetTotalRawBytes();
            }
        }
        workingMemory = WorkingArrayCopies * denseMemory + rows * (sizeof(NArrow::TRawReplaceKey) + PermutationBufferCopies * sizeof(ui64));
        SumOutputMemory += OutputBufferCopies * denseMemory;
    }
    MaxWorkingMemory = Max(MaxWorkingMemory, workingMemory);
    return SumBlobsMemory + GetConstructionMemory();
}

ui64 TTTLColumnEngineChanges::CalcMemoryForConstruction(const std::vector<std::shared_ptr<TPortionDataAccessor>>& accessors) const {
    if (!HasKeyRebuild) {
        return TBase::CalcMemoryForConstruction(accessors);
    }
    THashMap<ui64, const TPortionDataAccessor*> accessorsById;
    for (const auto& accessor : accessors) {
        accessorsById.emplace(accessor->GetPortionInfo().GetPortionId(), accessor.get());
    }
    TMemoryPredictor predictor;
    for (const auto& portion : PortionsToEvict) {
        predictor.AddPortion(portion.GetPortionInfo(), portion.GetFeatures(), accessorsById.at(portion.GetPortionInfo()->GetPortionId()));
    }
    // Input blobs have a separate reservation in TAskBlobDataResourceStep.
    return predictor.GetConstructionMemory();
}

void TTTLColumnEngineChanges::DoDebugString(TStringOutput& out) const {
    TBase::DoDebugString(out);
    out << "eviction=" << PortionsToEvict.size() << ";address=" << RWAddress.DebugString() << ";";
}

void TTTLColumnEngineChanges::DoStart(NColumnShard::TColumnShard& self) {
    Y_ABORT_UNLESS(PortionsToEvict.size() || GetPortionsToRemove().HasPortions());
    self.GetIndexAs<TColumnEngineForLogs>().GetActualizationController()->StartActualization(RWAddress);
}

void TTTLColumnEngineChanges::DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& /*context*/) {
    auto& engine = self.MutableIndexAs<TColumnEngineForLogs>();
    engine.GetActualizationController()->FinishActualization(RWAddress);
    if (IsAborted()) {
        THashMap<TInternalPathId, THashSet<ui64>> restoreIndexAddresses;
        for (auto&& i : PortionsToEvict) {
            AFL_VERIFY(restoreIndexAddresses[i.GetPortionInfo()->GetPathId()].emplace(i.GetPortionInfo()->GetPortionId()).second);
        }
        for (auto&& i : GetPortionsToRemove().GetPortions()) {
            AFL_VERIFY(restoreIndexAddresses[i.first.GetPathId()].emplace(i.first.GetPortionId()).second);
        }
        for (const auto& [original, replacement] : RewrittenPortions) {
            Y_UNUSED(replacement);
            restoreIndexAddresses[original->GetPathId()].emplace(original->GetPortionId());
        }
        engine.ReturnToIndexes(restoreIndexAddresses);
    }
}

std::optional<TWritePortionInfoWithBlobsResult> TTTLColumnEngineChanges::UpdateEvictedPortion(
    TPortionForEviction& info, NBlobOperations::NRead::TCompositeReadBlobs& srcBlobs, TConstructionContext& context) const {
    const TPortionInfo& portionInfo = *info.GetPortionInfo();
    auto& evictFeatures = info.GetFeatures();
    auto blobSchema = portionInfo.GetSchema(context.SchemaVersions);
    Y_ABORT_UNLESS(portionInfo.GetMeta().GetTierName() != evictFeatures.GetTargetTierName() ||
                   blobSchema->GetVersion() < evictFeatures.GetTargetScheme()->GetVersion() ||
                   portionInfo.GetMeta().GetRebuildDecimalPrimaryKey());

    auto portionWithBlobs = TReadPortionInfoWithBlobs::RestorePortion(
        GetPortionDataAccessor(info.GetPortionInfo()->GetPortionId()), srcBlobs, blobSchema->GetIndexInfo());
    std::optional<TWritePortionInfoWithBlobsResult> result =
        TReadPortionInfoWithBlobs::SyncPortion(std::move(portionWithBlobs), blobSchema, evictFeatures.GetTargetScheme(),
            evictFeatures.GetTargetTierName(), SaverContext.GetStoragesManager(), context.Counters.SplitterCounters);
    return std::move(result);
}

NKikimr::TConclusionStatus TTTLColumnEngineChanges::DoConstructBlobs(TConstructionContext& context) noexcept {
    Y_ABORT_UNLESS(!Blobs.IsEmpty());
    Y_ABORT_UNLESS(!PortionsToEvict.empty());

    for (auto&& info : PortionsToEvict) {
        if (auto pwb = UpdateEvictedPortion(info, Blobs, context)) {
            for (const auto& x : pwb->MutableBlobs()) {
                AFL_VERIFY(x.GetOperator()->GetStorageId() != IStoragesManager::LocalMetadataStorageId);
            }
            if (info.GetFeatures().GetRebuildDecimalPrimaryKey()) {
                RewrittenPortions.emplace_back(info.GetPortionInfo(), std::move(*pwb));
            } else {
                AddPortionToRemove(info.GetPortionInfo(), false);
                AppendedPortions.emplace_back(std::move(*pwb));
            }
        }
    }

    PortionsToEvict.clear();
    return TConclusionStatus::Success();
}

void TTTLColumnEngineChanges::DoCompile(TFinalizationContext& context) {
    NoAppendIsCorrect = !RewrittenPortions.empty();
    TBase::DoCompile(context);
}

void TTTLColumnEngineChanges::DoOnAfterCompile(const TFinalizationContext& context) {
    TBase::DoOnAfterCompile(context);
    for (auto& [original, replacement] : RewrittenPortions) {
        replacement.FinalizePortionConstructor(context.GetSnapshot());
        AFL_VERIFY(original->GetAddress() == replacement.GetPortionResult().GetPortionInfo().GetAddress());
    }
}

void TTTLColumnEngineChanges::DoWriteIndexOnExecute(NColumnShard::TColumnShard* self, TWriteIndexContext& context) {
    TBase::DoWriteIndexOnExecute(self, context);
    for (auto& [original, replacement] : RewrittenPortions) {
        AFL_VERIFY(original->GetMeta().HasIncompatibleDecimalKeyOrder());
        const auto& old = GetPortionDataAccessor(original->GetPortionId());
        old.RemoveFromDatabase(context.DBWrapper);
        const auto schema = original->GetSchema(context.EngineLogs.GetVersionedIndex());
        replacement.GetPortionResult().SaveToDatabase(context.DBWrapper, schema->GetIndexInfo().GetPKFirstColumnId(), false);
        if (self) {
            THashMap<TString, THashSet<TUnifiedBlobId>> blobs;
            old.FillBlobIdsByStorage(blobs, context.EngineLogs.GetVersionedIndex());
            for (const auto& [storage, ids] : blobs) {
                auto action = BlobsAction.GetRemoving(storage);
                for (const auto& id : ids) {
                    action->DeclareRemove((TTabletId)self->TabletID(), id);
                }
            }
        }
    }
}

void TTTLColumnEngineChanges::DoWriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) {
    TBase::DoWriteIndexOnComplete(self, context);
    for (auto& [original, replacement] : RewrittenPortions) {
        context.EngineLogs.ReplacePortion(original, replacement.GetPortionResultPtr());
    }
    if (!RewrittenPortions.empty()) {
        YDB_LOG_NOTICE("Decimal primary key repair applied", {"portions", RewrittenPortions.size()});
    }
}

NColumnShard::ECumulativeCounters TTTLColumnEngineChanges::GetCounterIndex(const bool isSuccess) const {
    return isSuccess ? NColumnShard::COUNTER_TTL_SUCCESS : NColumnShard::COUNTER_TTL_FAIL;
}

}   // namespace NKikimr::NOlap
