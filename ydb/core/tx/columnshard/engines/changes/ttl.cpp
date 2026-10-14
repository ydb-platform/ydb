#include "ttl.h"

#include <ydb/core/formats/arrow/accessor/dictionary/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
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

namespace {

bool NeedsDecodedSize(const NArrow::NAccessor::TColumnLoader& loader) {
    using EType = NArrow::NAccessor::IChunkedArray::EType;
    return !std::dynamic_pointer_cast<arrow::FixedWidthType>(loader.GetField()->type()) &&
           (loader.GetAccessorConstructor()->GetType() == EType::Dictionary ||
               loader.GetAccessorConstructor()->GetType() == EType::SubColumnsArray);
}

ui64 GetJsonPathMemory(const std::string_view name) {
    using namespace NBinaryJson;
    const auto path = NArrow::NAccessor::NSubColumns::ParseJsonPath(
        NArrow::NAccessor::NSubColumns::ToJsonPath(name.empty() ? TStringBuf("\"\"") : TStringBuf(name.data(), name.size())))
                          .DetachResult();
    ui64 result = 0;
    for (const auto& item : path.Items.PathItems) {
        result += sizeof(TMeta) + sizeof(TEntry) + sizeof(TKeyEntry) + sizeof(TSEntry) + item.size() + 1;
    }
    return result;
}

ui64 GetDecodedPayloadSize(const NArrow::NAccessor::IChunkedArray& data) {
    using namespace NArrow::NAccessor;
    if (const auto* dictionary = dynamic_cast<const TDictionaryArray*>(&data)) {
        ui64 result = 0;
        for (ui32 row = 0; row < data.GetRecordsCount(); ++row) {
            const auto value = std::static_pointer_cast<arrow::BaseBinaryScalar>(dictionary->GetScalar(row));
            if (value->is_valid) {
                result += value->value->size();
            }
        }
        return result;
    }
    const auto& subColumns = dynamic_cast<const TSubColumnsArray&>(data);
    using namespace NBinaryJson;
    constexpr ui64 DocumentOverhead = sizeof(THeader) + sizeof(TMeta) + sizeof(ui32);
    constexpr ui64 ValueOverhead = sizeof(TEntry) + sizeof(TSEntry) + sizeof(double) + 1;
    ui64 result = ui64(data.GetRecordsCount()) * DocumentOverhead;
    const auto& columns = subColumns.GetColumnsData();
    for (ui32 column = 0; column < columns.GetStats().GetColumnsCount(); ++column) {
        const ui64 pathMemory = GetJsonPathMemory(columns.GetStats().GetColumnName(column));
        for (auto it = columns.BuildIterator(column); it.IsValid(); it.Next()) {
            if (it.HasValue()) {
                result += pathMemory + ValueOverhead + it.GetValue().GetValueSize();
            }
        }
    }
    const auto& others = subColumns.GetOthersData();
    std::vector<ui64> pathsMemory;
    for (ui32 column = 0; column < others.GetStats().GetColumnsCount(); ++column) {
        pathsMemory.emplace_back(GetJsonPathMemory(others.GetStats().GetColumnName(column)));
    }
    for (auto it = others.BuildIterator(); it.IsValid(); it.Next()) {
        result += pathsMemory[it.GetKeyIndex()] + ValueOverhead + it.GetValue().GetValueSize();
    }
    // Shared ancestors and string-table entries are deliberately counted for every field.
    return result;
}

}   // namespace

ui64 TTTLColumnEngineChanges::TMemoryPredictor::AddPortion(const TPortionInfo::TConstPtr& portionInfo, const TPortionEvictionFeatures& features,
    const TPortionDataAccessor* accessor, const NBlobOperations::NRead::TCompositeReadBlobs* blobs) {
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
        THashSet<ui32> encodedColumns;
        for (const auto columnId : features.GetTargetScheme()->GetColumnIds()) {
            const auto loader = features.GetTargetScheme()->GetColumnLoaderVerified(columnId);
            const auto& type = loader->GetField()->type();
            denseMemory += arrow::BitUtil::BytesForBits(rows) + ArrayMetadataEstimateBytes;
            if (const auto fixed = std::dynamic_pointer_cast<arrow::FixedWidthType>(type)) {
                denseMemory += arrow::BitUtil::BytesForBits(rows * fixed->bit_width());
            } else {
                if (NeedsDecodedSize(*loader)) {
                    encodedColumns.emplace(columnId);
                }
                denseMemory += (rows + 1) * sizeof(ui64);   // offsets, including large binary
                if (const auto value = std::dynamic_pointer_cast<arrow::BaseBinaryScalar>(loader->GetDefaultValue()); value && value->is_valid) {
                    denseMemory += rows * value->value->size();
                }
            }
        }
        if (!encodedColumns.empty()) {
            if (accessor) {
                for (const auto& chunk : accessor->GetRecordsVerified()) {
                    if (encodedColumns.contains(chunk.GetColumnId())) {
                        AFL_VERIFY(blobs);
                        const auto& schema = *features.GetCurrentScheme();
                        const auto loader = schema.GetColumnLoaderVerified(chunk.GetColumnId());
                        const auto& blob =
                            blobs->GetBlobRangeVerified(portionInfo->GetColumnStorageId(chunk.GetColumnId(), schema.GetIndexInfo()),
                                accessor->RestoreBlobRange(chunk.GetBlobRange()));
                        const auto data = loader->ApplyVerified(
                            blob, chunk.GetMeta().GetRecordsCount(), std::nullopt, chunk.GetMeta().GetAdditionalAccessorData());
                        denseMemory += GetDecodedPayloadSize(*data);
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

ui64 TTTLColumnEngineChanges::CalcMemoryForConstruction(
    const std::vector<std::shared_ptr<TPortionDataAccessor>>& accessors, const NBlobOperations::NRead::TCompositeReadBlobs* blobs) const {
    if (!HasKeyRebuild) {
        return TBase::CalcMemoryForConstruction(accessors);
    }
    THashMap<ui64, const TPortionDataAccessor*> accessorsById;
    for (const auto& accessor : accessors) {
        accessorsById.emplace(accessor->GetPortionInfo().GetPortionId(), accessor.get());
    }
    TMemoryPredictor predictor;
    for (const auto& portion : PortionsToEvict) {
        predictor.AddPortion(portion.GetPortionInfo(), portion.GetFeatures(), accessorsById.at(portion.GetPortionInfo()->GetPortionId()), blobs);
    }
    // Input blobs have a separate reservation in TAskBlobDataResourceStep.
    return predictor.GetConstructionMemory();
}

ui64 TTTLColumnEngineChanges::GetMemoryForConstructionPreparation(const std::vector<std::shared_ptr<TPortionDataAccessor>>& accessors) const {
    if (!HasKeyRebuild) {
        return 0;
    }
    THashMap<ui64, const ISnapshotSchema*> schemas;
    for (const auto& portion : PortionsToEvict) {
        schemas.emplace(portion.GetPortionInfo()->GetPortionId(), portion.GetFeatures().GetCurrentScheme().get());
    }
    ui64 maxChunkMemory = 0;
    for (const auto& accessor : accessors) {
        const auto* schema = schemas.at(accessor->GetPortionInfo().GetPortionId());
        for (const auto& chunk : accessor->GetRecordsVerified()) {
            if (NeedsDecodedSize(*schema->GetColumnLoaderVerified(chunk.GetColumnId()))) {
                maxChunkMemory = Max(maxChunkMemory, ui64(chunk.GetMeta().GetRawBytes()) + chunk.GetBlobRange().Size);
            }
        }
    }
    // Decode one compact chunk at a time, without expanding dictionary values or building JSON documents.
    constexpr ui64 DecodeBufferCopies = 8;
    return DecodeBufferCopies * maxChunkMemory;
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
