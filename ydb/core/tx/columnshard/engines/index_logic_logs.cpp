#include "index_logic_logs.h"

#include <span>

namespace NKikimr::NOlap {

std::shared_ptr<arrow::RecordBatch> TIndexLogicBase::GetEffectiveKey(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                        const TIndexInfo& indexInfo) {
    const auto& key = indexInfo.GetIndexKey();
    auto resBatch = NArrow::ExtractColumns(batch, key);
    Y_VERIFY_S(resBatch, "Cannot extract effective key " << key->ToString()
        << " from batch " << batch->schema()->ToString());
    return resBatch;
}

std::shared_ptr<arrow::RecordBatch> TIndexationLogic::AddSpecials(const std::shared_ptr<arrow::RecordBatch>& srcBatch,
                                                const TIndexInfo& indexInfo, const TInsertedData& inserted) const {
    auto batch = TIndexInfo::AddSpecialColumns(srcBatch, inserted.GetSnapshot());
    Y_VERIFY(batch);

    return NArrow::ExtractColumns(batch, indexInfo.ArrowSchemaWithSpecials());
}

bool TEvictionLogic::UpdateEvictedPortion(TPortionInfo& portionInfo,
                                            TPortionEvictionFeatures& evictFeatures, const THashMap<TBlobRange, TString>& srcBlobs,
                                            std::vector<TColumnRecord>& evictedRecords, std::vector<TString>& newBlobs) const {
    Y_VERIFY(portionInfo.TierName != evictFeatures.TargetTierName);

    auto* tiering = GetTieringMap().FindPtr(evictFeatures.PathId);
    Y_VERIFY(tiering);
    auto compression = tiering->GetCompression(evictFeatures.TargetTierName);
    if (!compression) {
        // Noting to recompress. We have no other kinds of evictions yet.
        portionInfo.TierName = evictFeatures.TargetTierName;
        evictFeatures.DataChanges = false;
        return true;
    }

    Y_VERIFY(!evictFeatures.NeedExport);

    TPortionInfo undo = portionInfo;

    auto blobSchema = SchemaVersions.GetSchema(undo.GetSnapshot());
    auto resultSchema = SchemaVersions.GetLastSchema();
    auto batch = portionInfo.AssembleInBatch(*blobSchema, *resultSchema, srcBlobs);

    size_t undoSize = newBlobs.size();
    TSaverContext saverContext;
    saverContext.SetTierName(evictFeatures.TargetTierName).SetExternalCompression(compression);
    for (auto& rec : portionInfo.Records) {
        auto pos = resultSchema->GetFieldIndex(rec.ColumnId);
        Y_VERIFY(pos >= 0);
        auto field = resultSchema->GetFieldByIndex(pos);
        auto columnSaver = resultSchema->GetColumnSaver(rec.ColumnId, saverContext);

        auto blob = TPortionInfo::SerializeColumn(batch->GetColumnByName(field->name()), field, columnSaver);
        if (blob.size() >= TPortionInfo::BLOB_BYTES_LIMIT) {
            portionInfo = undo;
            newBlobs.resize(undoSize);
            return false;
        }
        newBlobs.emplace_back(std::move(blob));
        rec.BlobRange = TBlobRange{};
    }

    for (auto& rec : undo.Records) {
        evictedRecords.emplace_back(std::move(rec));
    }

    portionInfo.AddMetadata(*resultSchema, batch, evictFeatures.TargetTierName);
    return true;
}

class TSplitLimiter {
private:
    static const inline double ReduceCorrectionKff = 0.9;
    static const inline double IncreaseCorrectionKff = 1.1;
    static const inline ui64 ExpectedBlobSize = 6 * 1024 * 1024;
    static const inline ui64 MinBlobSize = 1 * 1024 * 1024;

    const NColumnShard::TIndexationCounters Counters;
    ui32 BaseStepRecordsCount = 0;
    ui32 CurrentStepRecordsCount = 0;
    std::shared_ptr<arrow::RecordBatch> Batch;
    std::vector<TColumnSummary> SortedColumnIds;
    ui32 Position = 0;
    ISnapshotSchema::TPtr Schema;
public:
    TSplitLimiter(const TGranuleMeta* granuleMeta, const NColumnShard::TIndexationCounters& counters,
        ISnapshotSchema::TPtr schema, const std::shared_ptr<arrow::RecordBatch> batch)
        : Counters(counters)
        , Batch(batch)
        , Schema(schema)
    {
        if (granuleMeta && granuleMeta->GetAdditiveSummary().GetOther().GetRecordsCount()) {
            SortedColumnIds = granuleMeta->GetHardSummary().GetColumnIdsSortedBySizeDescending();
            const auto biggestColumn = SortedColumnIds.front();
            Y_VERIFY(biggestColumn.GetPackedBlobsSize());
            const double expectedPackedRecordSize = 1.0 * biggestColumn.GetPackedBlobsSize() / granuleMeta->GetAdditiveSummary().GetOther().GetRecordsCount();
            BaseStepRecordsCount = ExpectedBlobSize / expectedPackedRecordSize;
            for (ui32 i = 1; i < SortedColumnIds.size(); ++i) {
                Y_VERIFY(SortedColumnIds[i - 1].GetPackedBlobsSize() >= SortedColumnIds[i].GetPackedBlobsSize());
            }
            if (BaseStepRecordsCount > batch->num_rows()) {
                BaseStepRecordsCount = batch->num_rows();
            } else {
                BaseStepRecordsCount = batch->num_rows() / (ui32)(batch->num_rows() / BaseStepRecordsCount);
                if (BaseStepRecordsCount * expectedPackedRecordSize > TCompactionLimits::MAX_BLOB_SIZE) {
                    BaseStepRecordsCount = ExpectedBlobSize / expectedPackedRecordSize;
                }
            }
        } else {
            for (auto&& i : Schema->GetIndexInfo().GetColumnIds()) {
                SortedColumnIds.emplace_back(TColumnSummary(i));
            }
            BaseStepRecordsCount = batch->num_rows();
        }
        BaseStepRecordsCount = std::min<ui32>(BaseStepRecordsCount, Batch->num_rows());
        Y_VERIFY(BaseStepRecordsCount);
        CurrentStepRecordsCount = BaseStepRecordsCount;
    }

    bool Next(std::vector<TString>& portionBlobs, std::shared_ptr<arrow::RecordBatch>& batch, const TSaverContext& saverContext) {
        if (Position == Batch->num_rows()) {
            return false;
        }

        portionBlobs.resize(Schema->GetSchema()->num_fields());
        while (true) {
            Y_VERIFY(Position < Batch->num_rows());
            std::shared_ptr<arrow::RecordBatch> currentBatch;
            if (Batch->num_rows() - Position < CurrentStepRecordsCount * 1.1) {
                currentBatch = Batch->Slice(Position, Batch->num_rows() - Position);
            } else {
                currentBatch = Batch->Slice(Position, CurrentStepRecordsCount);
            }

            ui32 fillCounter = 0;
            for (const auto& columnSummary : SortedColumnIds) {
                const TString& columnName = Schema->GetIndexInfo().GetColumnName(columnSummary.GetColumnId());
                const int idx = Schema->GetFieldIndex(columnSummary.GetColumnId());
                Y_VERIFY(idx >= 0);
                auto field = Schema->GetFieldByIndex(idx);
                Y_VERIFY(field);
                auto array = currentBatch->GetColumnByName(columnName);
                Y_VERIFY(array);
                auto columnSaver = Schema->GetColumnSaver(columnSummary.GetColumnId(), saverContext);
                TString blob = TPortionInfo::SerializeColumn(array, field, columnSaver);
                if (blob.size() >= TCompactionLimits::MAX_BLOB_SIZE) {
                    Counters.TrashDataSerializationBytes->Add(blob.size());
                    Counters.TrashDataSerialization->Add(1);
                    Counters.TrashDataSerializationHistogramBytes->Collect(blob.size());
                    const double kffNew = 1.0 * ExpectedBlobSize / blob.size() * ReduceCorrectionKff;
                    CurrentStepRecordsCount = currentBatch->num_rows() * kffNew;
                    Y_VERIFY(CurrentStepRecordsCount);
                    break;
                } else {
                    Counters.CorrectDataSerializationBytes->Add(blob.size());
                    Counters.CorrectDataSerialization->Add(1);
                }

                portionBlobs[idx] = std::move(blob);
                ++fillCounter;
            }

            if (fillCounter == portionBlobs.size()) {
                Y_VERIFY(fillCounter == portionBlobs.size());
                Position += currentBatch->num_rows();
                Y_VERIFY(Position <= Batch->num_rows());
                ui64 maxBlobSize = 0;
                for (auto&& i : portionBlobs) {
                    Counters.SplittedPortionColumnSize->Collect(i.size());
                    maxBlobSize = std::max<ui64>(maxBlobSize, i.size());
                }
                batch = currentBatch;
                if (maxBlobSize < MinBlobSize) {
                    if ((Position != currentBatch->num_rows() || Position != Batch->num_rows())) {
                        Counters.SplittedPortionLargestColumnSize->Collect(maxBlobSize);
                        Counters.TooSmallBlob->Add(1);
                        if (Position == Batch->num_rows()) {
                            Counters.TooSmallBlobFinish->Add(1);
                        }
                        if (Position == currentBatch->num_rows()) {
                            Counters.TooSmallBlobStart->Add(1);
                        }
                    } else {
                        Counters.SimpleSplitPortionLargestColumnSize->Collect(maxBlobSize);
                    }
                    CurrentStepRecordsCount = currentBatch->num_rows() * IncreaseCorrectionKff;
                } else {
                    Counters.SplittedPortionLargestColumnSize->Collect(maxBlobSize);
                }
                return true;
            }
        }
    }
};

std::vector<TPortionInfo> TIndexLogicBase::MakeAppendedPortions(const ui64 pathId,
                                                            const std::shared_ptr<arrow::RecordBatch> batch,
                                                            const ui64 granule,
                                                            const TSnapshot& snapshot,
                                                            std::vector<TString>& blobs, const TGranuleMeta* granuleMeta) const {
    Y_VERIFY(batch->num_rows());

    auto resultSchema = SchemaVersions.GetSchema(snapshot);
    std::vector<TPortionInfo> out;

    TString tierName;
    std::optional<NArrow::TCompression> compression;
    if (pathId) {
        if (auto* tiering = GetTieringMap().FindPtr(pathId)) {
            tierName = tiering->GetHottestTierName();
            if (const auto& tierCompression = tiering->GetCompression(tierName)) {
                compression = *tierCompression;
            }
        }
    }
    TSaverContext saverContext;
    saverContext.SetTierName(tierName).SetExternalCompression(compression);

    TSplitLimiter limiter(granuleMeta, Counters, resultSchema, batch);

    std::vector<TString> portionBlobs;
    std::shared_ptr<arrow::RecordBatch> portionBatch;
    while (limiter.Next(portionBlobs, portionBatch, saverContext)) {
        TPortionInfo portionInfo;
        portionInfo.Records.reserve(resultSchema->GetSchema()->num_fields());
        for (auto&& f : resultSchema->GetSchema()->fields()) {
            const ui32 columnId = resultSchema->GetIndexInfo().GetColumnId(f->name());
            TColumnRecord record = TColumnRecord::Make(granule, columnId, snapshot, 0);
            portionInfo.AppendOneChunkColumn(std::move(record));
        }
        for (auto&& i : portionBlobs) {
            blobs.emplace_back(i);
        }
        portionInfo.AddMetadata(*resultSchema, portionBatch, tierName);
        out.emplace_back(std::move(portionInfo));
    }

    return out;
}

std::pair<std::vector<std::shared_ptr<arrow::RecordBatch>>, TSnapshot>
TCompactionLogic::PortionsToBatches(const std::vector<TPortionInfo>& portions, const THashMap<TBlobRange, TString>& blobs,
                                    const bool insertedOnly) const {
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    batches.reserve(portions.size());
    const auto resultSchema = SchemaVersions.GetLastSchema();
    TSnapshot maxSnapshot = resultSchema->GetSnapshot();
    for (const auto& portionInfo : portions) {
        if (!insertedOnly || portionInfo.IsInserted()) {
            const auto blobSchema = SchemaVersions.GetSchema(portionInfo.GetSnapshot());

            batches.push_back(portionInfo.AssembleInBatch(*blobSchema, *resultSchema, blobs));

            if (maxSnapshot < portionInfo.GetSnapshot()) {
                maxSnapshot = portionInfo.GetSnapshot();
            }
        }
    }
    return std::make_pair(std::move(batches), maxSnapshot);
}

THashMap<ui64, std::shared_ptr<arrow::RecordBatch>> TIndexLogicBase::SliceIntoGranules(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                                                        const std::vector<std::pair<TMark, ui64>>& granules,
                                                                                        const TIndexInfo& indexInfo) {
    Y_VERIFY(batch);
    if (batch->num_rows() == 0) {
        return {};
    }

    THashMap<ui64, std::shared_ptr<arrow::RecordBatch>> out;

    if (granules.size() == 1) {
        out.emplace(granules[0].second, batch);
    } else {
        const auto effKey = GetEffectiveKey(batch, indexInfo);
        Y_VERIFY(effKey->num_columns() && effKey->num_rows());

        std::vector<NArrow::TRawReplaceKey> keys;
        {
            const auto& columns = effKey->columns();
            keys.reserve(effKey->num_rows());
            for (i64 i = 0; i < effKey->num_rows(); ++i) {
                keys.emplace_back(NArrow::TRawReplaceKey(&columns, i));
            }
        }

        i64 offset = 0;
        for (size_t i = 0; i < granules.size() && offset < effKey->num_rows(); ++i) {
            const i64 end = (i + 1 == granules.size())
                                // Just take the number of elements in the key column for the last granule.
                                ? effKey->num_rows()
                                // Locate position of the next granule in the key.
                                : NArrow::LowerBound(keys, granules[i + 1].first.GetBorder(), offset);

            if (const i64 size = end - offset) {
                Y_VERIFY(out.emplace(granules[i].second, batch->Slice(offset, size)).second);
            }

            offset = end;
        }
    }
    return out;
}

TConclusion<std::vector<TString>> TIndexationLogic::DoApply(std::shared_ptr<TColumnEngineChanges> indexChanges) const noexcept {
    auto changes = std::static_pointer_cast<TColumnEngineForLogs::TChanges>(indexChanges);
    Y_VERIFY(!changes->DataToIndex.empty());
    Y_VERIFY(changes->AppendedPortions.empty());

    auto maxSnapshot = TSnapshot::Zero();
    for (auto& inserted : changes->DataToIndex) {
        TSnapshot insertSnap = inserted.GetSnapshot();
        Y_VERIFY(insertSnap.Valid());
        if (insertSnap > maxSnapshot) {
            maxSnapshot = insertSnap;
        }
    }
    Y_VERIFY(maxSnapshot.Valid());

    auto resultSchema = SchemaVersions.GetSchema(maxSnapshot);
    Y_VERIFY(resultSchema->GetIndexInfo().IsSorted());

    THashMap<ui64, std::vector<std::shared_ptr<arrow::RecordBatch>>> pathBatches;
    for (auto& inserted : changes->DataToIndex) {
        TBlobRange blobRange(inserted.BlobId, 0, inserted.BlobId.BlobSize());

        auto blobSchema = SchemaVersions.GetSchema(inserted.GetSchemaSnapshot());
        auto& indexInfo = blobSchema->GetIndexInfo();
        Y_VERIFY(indexInfo.IsSorted());

        std::shared_ptr<arrow::RecordBatch> batch;
        if (auto it = changes->CachedBlobs.find(inserted.BlobId); it != changes->CachedBlobs.end()) {
            batch = it->second;
        } else if (auto* blobData = changes->Blobs.FindPtr(blobRange)) {
            Y_VERIFY(!blobData->empty(), "Blob data not present");
            // Prepare batch
            batch = NArrow::DeserializeBatch(*blobData, indexInfo.ArrowSchema());
            if (!batch) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)
                    ("event", "cannot_parse")
                    ("data_snapshot", TStringBuilder() << inserted.GetSnapshot())
                    ("index_snapshot", TStringBuilder() << blobSchema->GetSnapshot());
            }
        } else {
            Y_VERIFY(blobData, "Data for range %s has not been read", blobRange.ToString().c_str());
        }
        Y_VERIFY(batch);

        batch = AddSpecials(batch, blobSchema->GetIndexInfo(), inserted);
        batch = resultSchema->NormalizeBatch(*blobSchema, batch);
        pathBatches[inserted.PathId].push_back(batch);
        Y_VERIFY_DEBUG(NArrow::IsSorted(pathBatches[inserted.PathId].back(), resultSchema->GetIndexInfo().GetReplaceKey()));
    }
    std::vector<TString> blobs;

    for (auto& [pathId, batches] : pathBatches) {
        changes->AddPathIfNotExists(pathId);

        // We could merge data here cause tablet limits indexing data portions
        auto merged = NArrow::CombineSortedBatches(batches, resultSchema->GetIndexInfo().SortReplaceDescription());
        Y_VERIFY(merged);
        Y_VERIFY_DEBUG(NArrow::IsSortedAndUnique(merged, resultSchema->GetIndexInfo().GetReplaceKey()));

        auto granuleBatches = SliceIntoGranules(merged, changes->PathToGranule[pathId], resultSchema->GetIndexInfo());
        for (auto& [granule, batch] : granuleBatches) {
            auto portions = MakeAppendedPortions(pathId, batch, granule, maxSnapshot, blobs, changes->GetGranuleMeta());
            Y_VERIFY(portions.size() > 0);
            for (auto& portion : portions) {
                changes->AppendedPortions.emplace_back(std::move(portion));
            }
        }
    }

    Y_VERIFY(changes->PathToGranule.size() == pathBatches.size());
    return blobs;
}

std::pair<std::shared_ptr<arrow::RecordBatch>, TSnapshot> TCompactionLogic::CompactInOneGranule(ui64 granule,
                                                                            const std::vector<TPortionInfo>& portions,
                                                                            const THashMap<TBlobRange, TString>& blobs) const {
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    batches.reserve(portions.size());

    auto resultSchema = SchemaVersions.GetLastSchema();

    TSnapshot maxSnapshot = resultSchema->GetSnapshot();
    for (auto& portionInfo : portions) {
        Y_VERIFY(!portionInfo.Empty());
        Y_VERIFY(portionInfo.Granule() == granule);
        auto blobSchema = SchemaVersions.GetSchema(portionInfo.GetSnapshot());
        auto batch = portionInfo.AssembleInBatch(*blobSchema, *resultSchema, blobs);
        batches.push_back(batch);
        if (portionInfo.GetSnapshot() > maxSnapshot) {
            maxSnapshot = portionInfo.GetSnapshot();
        }
    }

    auto sortedBatch = NArrow::CombineSortedBatches(batches, resultSchema->GetIndexInfo().SortReplaceDescription());
    Y_VERIFY_DEBUG(NArrow::IsSortedAndUnique(sortedBatch, resultSchema->GetIndexInfo().GetReplaceKey()));

    return std::make_pair(sortedBatch, maxSnapshot);
}

std::vector<TString> TCompactionLogic::CompactInGranule(std::shared_ptr<TColumnEngineForLogs::TChanges> changes) const {
    const ui64 pathId = changes->SrcGranule->PathId;
    std::vector<TString> blobs;
    auto& switchedPortions = changes->SwitchedPortions;
    Y_VERIFY(switchedPortions.size());

    ui64 granule = switchedPortions[0].Granule();
    auto [batch, maxSnapshot] = CompactInOneGranule(granule, switchedPortions, changes->Blobs);

    auto resultSchema = SchemaVersions.GetLastSchema();
    std::vector<TPortionInfo> portions;
    if (!changes->MergeBorders.Empty()) {
        Y_VERIFY(changes->MergeBorders.GetOrderedMarks().size() > 1);
        auto slices = changes->MergeBorders.SliceIntoGranules(batch, resultSchema->GetIndexInfo());
        portions.reserve(slices.size());

        for (auto& [_, slice] : slices) {
            if (!slice || slice->num_rows() == 0) {
                continue;
            }
            auto tmp = MakeAppendedPortions(pathId, slice, granule, maxSnapshot, blobs, changes->GetGranuleMeta());
            for (auto&& portionInfo : tmp) {
                portions.emplace_back(std::move(portionInfo));
            }
        }
    } else {
        portions = MakeAppendedPortions(pathId, batch, granule, maxSnapshot, blobs, changes->GetGranuleMeta());
    }

    Y_VERIFY(portions.size() > 0);
    Y_VERIFY(changes->AppendedPortions.empty());
    // Set appended portions.
    changes->AppendedPortions.swap(portions);

    return blobs;
}

std::vector<std::pair<TMark, std::shared_ptr<arrow::RecordBatch>>>
TCompactionLogic::SliceGranuleBatches(const TIndexInfo& indexInfo,
                                        const TColumnEngineForLogs::TChanges& changes,
                                        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
                                        const TMark& ts0) const {
    std::vector<std::pair<TMark, std::shared_ptr<arrow::RecordBatch>>> out;

    // Extract unique effective keys and their counts
    i64 numRows = 0;
    TMap<NArrow::TReplaceKey, ui32> uniqKeyCount;
    for (const auto& batch : batches) {
        Y_VERIFY(batch);
        if (batch->num_rows() == 0) {
            continue;
        }

        numRows += batch->num_rows();

        const auto effKey = GetEffectiveKey(batch, indexInfo);
        Y_VERIFY(effKey->num_columns() && effKey->num_rows());

        auto effColumns = std::make_shared<NArrow::TArrayVec>(effKey->columns());
        for (int row = 0; row < effKey->num_rows(); ++row) {
            ++uniqKeyCount[NArrow::TReplaceKey(effColumns, row)];
        }
    }

    Y_VERIFY(uniqKeyCount.size());
    auto minTs = uniqKeyCount.begin()->first;
    auto maxTs = uniqKeyCount.rbegin()->first;
    Y_VERIFY(minTs >= ts0.GetBorder());

    // It's an estimation of needed count cause numRows calculated before key replaces
    ui32 numSplitInto = changes.NumSplitInto(numRows);
    ui32 rowsInGranule = numRows / numSplitInto;
    Y_VERIFY(rowsInGranule);

    // Cannot split in case of one unique key
    if (uniqKeyCount.size() == 1) {
        // We have to split big batch of same key in several portions
        auto merged = NArrow::MergeSortedBatches(batches, indexInfo.SortReplaceDescription(), rowsInGranule);
        for (auto& batch : merged) {
            Y_VERIFY_DEBUG(NArrow::IsSortedAndUnique(batch, indexInfo.GetReplaceKey()));
            out.emplace_back(ts0, batch);
        }
        return out;
    }

    // Make split borders from uniq keys
    std::vector<NArrow::TReplaceKey> borders;
    borders.reserve(numRows / rowsInGranule);
    {
        ui32 sumRows = 0;
        for (auto& [ts, num] : uniqKeyCount) {
            if (sumRows >= rowsInGranule) {
                borders.emplace_back(ts);
                sumRows = 0;
            }
            sumRows += num;
        }
        if (borders.empty()) {
            borders.emplace_back(maxTs); // huge trailing key
        }
        Y_VERIFY(borders.size());
    }

    // Find offsets in source batches
    std::vector<std::vector<int>> offsets(batches.size()); // vec[batch][border] = offset
    for (size_t i = 0; i < batches.size(); ++i) {
        const auto& batch = batches[i];
        auto& batchOffsets = offsets[i];
        batchOffsets.reserve(borders.size() + 1);

        const auto effKey = GetEffectiveKey(batch, indexInfo);
        Y_VERIFY(effKey->num_columns() && effKey->num_rows());

        std::vector<NArrow::TRawReplaceKey> keys;
        {
            const auto& columns = effKey->columns();
            keys.reserve(effKey->num_rows());
            for (i64 i = 0; i < effKey->num_rows(); ++i) {
                keys.emplace_back(NArrow::TRawReplaceKey(&columns, i));
            }
        }

        batchOffsets.push_back(0);
        for (const auto& border : borders) {
            int offset = NArrow::LowerBound(keys, border, batchOffsets.back());
            Y_VERIFY(offset >= batchOffsets.back());
            batchOffsets.push_back(offset);
        }

        Y_VERIFY(batchOffsets.size() == borders.size() + 1);
    }

    // Make merge-sorted granule batch for each splitted granule
    for (ui32 granuleNo = 0; granuleNo < borders.size() + 1; ++granuleNo) {
        std::vector<std::shared_ptr<arrow::RecordBatch>> granuleBatches;
        granuleBatches.reserve(batches.size());

        // Extract granule: slice source batches with offsets
        i64 granuleNumRows = 0;
        for (size_t i = 0; i < batches.size(); ++i) {
            const auto& batch = batches[i];
            auto& batchOffsets = offsets[i];

            int offset = batchOffsets[granuleNo];
            int end = batch->num_rows();
            if (granuleNo < borders.size()) {
                end = batchOffsets[granuleNo + 1];
            }
            int size = end - offset;
            Y_VERIFY(size >= 0);

            if (size) {
                auto slice = batch->Slice(offset, size);
                Y_VERIFY(slice->num_rows());
                granuleNumRows += slice->num_rows();
#if 1 // Check correctness
                const auto effKey = GetEffectiveKey(slice, indexInfo);
                Y_VERIFY(effKey->num_columns() && effKey->num_rows());

                auto startKey = granuleNo ? borders[granuleNo - 1] : minTs;
                Y_VERIFY(NArrow::TReplaceKey::FromBatch(effKey, 0) >= startKey);

                NArrow::TReplaceKey lastSliceKey = NArrow::TReplaceKey::FromBatch(effKey, effKey->num_rows() - 1);
                if (granuleNo < borders.size() - 1) {
                    const auto& endKey = borders[granuleNo];
                    Y_VERIFY(lastSliceKey < endKey);
                } else {
                    Y_VERIFY(lastSliceKey <= maxTs);
                }
#endif
                Y_VERIFY_DEBUG(NArrow::IsSorted(slice, indexInfo.GetReplaceKey()));
                granuleBatches.emplace_back(slice);
            }
        }

        // Merge slices. We have to split a big key batches in several ones here.
        if (granuleNumRows > 4 * rowsInGranule) {
            granuleNumRows = rowsInGranule;
        }
        auto merged = NArrow::MergeSortedBatches(granuleBatches, indexInfo.SortReplaceDescription(), granuleNumRows);
        for (auto& batch : merged) {
            Y_VERIFY_DEBUG(NArrow::IsSortedAndUnique(batch, indexInfo.GetReplaceKey()));

            auto startKey = ts0.GetBorder();
            if (granuleNo) {
                startKey = borders[granuleNo - 1];
            }
#if 1 // Check correctness
            const auto effKey = GetEffectiveKey(batch, indexInfo);
            Y_VERIFY(effKey->num_columns() && effKey->num_rows());

            Y_VERIFY(NArrow::TReplaceKey::FromBatch(effKey, 0) >= startKey);
#endif
            out.emplace_back(TMark(startKey), batch);
        }
    }

    return out;
}

ui64 TCompactionLogic::TryMovePortions(const TMark& ts0,
                                        std::vector<TPortionInfo>& portions,
                                        std::vector<std::pair<TMark, ui64>>& tsIds,
                                        std::vector<std::pair<TPortionInfo, ui64>>& toMove) const {
    std::vector<TPortionInfo*> partitioned(portions.size());
    // Split portions by putting the inserted portions in the original order
    // at the beginning of the buffer and the compacted portions at the end.
    // The compacted portions will be put in the reversed order, but it will be sorted later.
    const auto [inserted, compacted] = [&]() {
        size_t l = 0;
        size_t r = portions.size();

        for (auto& portionInfo : portions) {
            partitioned[(portionInfo.IsInserted() ? l++ : --r)] = &portionInfo;
        }

        return std::make_tuple(std::span(partitioned.begin(), l), std::span(partitioned.begin() + l, partitioned.end()));
    }();

    Counters.AnalizeCompactedPortions->Add(compacted.size());
    Counters.AnalizeInsertedPortions->Add(inserted.size());
    for (auto&& i : portions) {
        if (i.IsInserted()) {
            Counters.AnalizeInsertedBytes->Add(i.BlobsBytes());
        } else {
            Counters.AnalizeCompactedBytes->Add(i.BlobsBytes());
        }
    }

    // Do nothing if there are less than two compacted portions.
    if (compacted.size() < 2) {
        return 0;
    }
    // Order compacted portions by primary key.
    std::sort(compacted.begin(), compacted.end(), [](const TPortionInfo* a, const TPortionInfo* b) {
        return a->IndexKeyStart() < b->IndexKeyStart();
    });
    for (auto&& i : inserted) {
        Counters.RepackedInsertedPortionBytes->Add(i->BlobsBytes());
    }
    Counters.RepackedInsertedPortions->Add(inserted.size());

    // Check that there are no gaps between two adjacent portions in term of primary key range.
    for (size_t i = 0; i < compacted.size() - 1; ++i) {
        if (compacted[i]->IndexKeyEnd() >= compacted[i + 1]->IndexKeyStart()) {
            for (auto&& c : compacted) {
                Counters.SkipPortionBytesMoveThroughIntersection->Add(c->BlobsBytes());
            }

            Counters.SkipPortionsMoveThroughIntersection->Add(compacted.size());
            Counters.RepackedCompactedPortions->Add(compacted.size());
            return 0;
        }
    }

    toMove.reserve(compacted.size());
    ui64 numRows = 0;
    ui32 counter = 0;
    for (auto* portionInfo : compacted) {
        ui32 rows = portionInfo->NumRows();
        Y_VERIFY(rows);
        numRows += rows;
        Counters.MovedPortionBytes->Add(portionInfo->BlobsBytes());
        tsIds.emplace_back((counter ? TMark(portionInfo->IndexKeyStart()) : ts0), counter + 1);
        toMove.emplace_back(std::move(*portionInfo), counter);
        ++counter;
        // Ensure that std::move will take an effect.
        static_assert(std::swappable<decltype(*portionInfo)>);
    }
    Counters.MovedPortions->Add(toMove.size());

    std::vector<TPortionInfo> out;
    out.reserve(inserted.size());
    for (auto* portionInfo : inserted) {
        out.emplace_back(std::move(*portionInfo));
        // Ensure that std::move will take an effect.
        static_assert(std::swappable<decltype(*portionInfo)>);
    }
    portions.swap(out);

    return numRows;
}

std::vector<TString> TCompactionLogic::CompactSplitGranule(const std::shared_ptr<TColumnEngineForLogs::TChanges>& changes) const {
    const ui64 pathId = changes->SrcGranule->PathId;
    const TMark ts0 = changes->SrcGranule->Mark;
    std::vector<TPortionInfo>& portions = changes->SwitchedPortions;

    std::vector<std::pair<TMark, ui64>> tsIds;
    ui64 movedRows = TryMovePortions(ts0, portions, tsIds, changes->PortionsToMove);
    auto [srcBatches, maxSnapshot] = PortionsToBatches(portions, changes->Blobs, movedRows != 0);
    Y_VERIFY(srcBatches.size() == portions.size());

    std::vector<TString> blobs;
    auto resultSchema = SchemaVersions.GetLastSchema();
    if (movedRows) {
        Y_VERIFY(changes->PortionsToMove.size() >= 2);
        Y_VERIFY(changes->PortionsToMove.size() == tsIds.size());
        Y_VERIFY(tsIds.begin()->first == ts0);

        // Calculate total number of rows.
        ui64 numRows = movedRows;
        for (const auto& batch : srcBatches) {
            numRows += batch->num_rows();
        }

        // Recalculate new granules borders (if they are larger then portions)
        ui32 numSplitInto = changes->NumSplitInto(numRows);
        if (numSplitInto < tsIds.size()) {
            const ui32 rowsInGranule = numRows / numSplitInto;
            Y_VERIFY(rowsInGranule);

            std::vector<std::pair<TMark, ui64>> newTsIds;
            ui32 tmpGranule = 0;
            ui32 sumRows = 0;
            // Always insert mark of the source granule at the beginning.
            newTsIds.emplace_back(ts0, 1);

            for (size_t i = 0, end = tsIds.size(); i != end; ++i) {
                const TMark& ts = tsIds[i].first;
                // Make new granule if the current number of rows is exceeded the allowed number of rows in the granule
                // or there is the end of the ids and nothing was inserted so far.
                if (sumRows >= rowsInGranule || (i + 1 == end && newTsIds.size() == 1)) {
                    ++tmpGranule;
                    newTsIds.emplace_back(ts, tmpGranule + 1);
                    sumRows = 0;
                }

                auto& toMove = changes->PortionsToMove[i];
                sumRows += toMove.first.NumRows();
                toMove.second = tmpGranule;
            }

            tsIds.swap(newTsIds);
        }
        Y_VERIFY(tsIds.size() > 1);
        Y_VERIFY(tsIds[0] == std::make_pair(ts0, ui64(1)));
        TColumnEngineForLogs::TMarksGranules marksGranules(std::move(tsIds));

        // Slice inserted portions with granules' borders
        THashMap<ui64, std::vector<std::shared_ptr<arrow::RecordBatch>>> idBatches;
        std::vector<TPortionInfo*> toSwitch;
        toSwitch.reserve(portions.size());
        for (size_t i = 0; i < portions.size(); ++i) {
            auto& portion = portions[i];
            auto& batch = srcBatches[i];
            auto slices = marksGranules.SliceIntoGranules(batch, resultSchema->GetIndexInfo());

            THashSet<ui64> ids;
            for (auto& [id, slice] : slices) {
                if (slice && slice->num_rows()) {
                    ids.insert(id);
                    idBatches[id].emplace_back(std::move(slice));
                }
            }

            // Optimization: move not splitted inserted portions. Do not reappend them.
            if (ids.size() == 1) {
                ui64 id = *ids.begin();
                idBatches[id].resize(idBatches[id].size() - 1);
                ui64 tmpGranule = id - 1;
                changes->PortionsToMove.emplace_back(std::move(portion), tmpGranule);
            } else {
                toSwitch.push_back(&portion);
            }
        }

        // Update switchedPortions if we have moves
        if (toSwitch.size() != portions.size()) {
            std::vector<TPortionInfo> tmp;
            tmp.reserve(toSwitch.size());
            for (auto* portionInfo : toSwitch) {
                tmp.emplace_back(std::move(*portionInfo));
            }
            portions.swap(tmp);
        }

        for (const auto& [mark, id] : marksGranules.GetOrderedMarks()) {
            ui64 tmpGranule = changes->SetTmpGranule(pathId, mark);
            for (const auto& batch : idBatches[id]) {
                // Cannot set snapshot here. It would be set in committing transaction in ApplyChanges().
                auto newPortions = MakeAppendedPortions(pathId, batch, tmpGranule, maxSnapshot, blobs, changes->GetGranuleMeta());
                Y_VERIFY(newPortions.size() > 0);
                for (auto& portion : newPortions) {
                    changes->AppendedPortions.emplace_back(std::move(portion));
                }
            }
        }
    } else {
        auto batches = SliceGranuleBatches(resultSchema->GetIndexInfo(), *changes, srcBatches, ts0);

        changes->SetTmpGranule(pathId, ts0);
        for (auto& [ts, batch] : batches) {
            // Tmp granule would be updated to correct value in ApplyChanges()
            ui64 tmpGranule = changes->SetTmpGranule(pathId, ts);

            // Cannot set snapshot here. It would be set in committing transaction in ApplyChanges().
            auto portions = MakeAppendedPortions(pathId, batch, tmpGranule, maxSnapshot, blobs, changes->GetGranuleMeta());
            Y_VERIFY(portions.size() > 0);
            for (auto& portion : portions) {
                changes->AppendedPortions.emplace_back(std::move(portion));
            }
        }
    }

    return blobs;
}

bool TCompactionLogic::IsSplit(std::shared_ptr<TColumnEngineChanges> changes) {
    auto castedChanges = std::static_pointer_cast<TColumnEngineForLogs::TChanges>(changes);
    return !castedChanges->CompactionInfo->InGranule();
}

TConclusion<std::vector<TString>> TCompactionLogic::DoApply(std::shared_ptr<TColumnEngineChanges> changes) const noexcept {
    Y_VERIFY(changes);
    Y_VERIFY(changes->CompactionInfo);
    Y_VERIFY(changes->DataToIndex.empty());       // not used
    Y_VERIFY(!changes->Blobs.empty());            // src data
    Y_VERIFY(!changes->SwitchedPortions.empty()); // src meta
    Y_VERIFY(changes->AppendedPortions.empty());  // dst meta

    auto castedChanges = std::static_pointer_cast<TColumnEngineForLogs::TChanges>(changes);
    if (!IsSplit(castedChanges)) {
        return CompactInGranule(castedChanges);
    } else {
        return CompactSplitGranule(castedChanges);
    }
}

TConclusion<std::vector<TString>> TEvictionLogic::DoApply(std::shared_ptr<TColumnEngineChanges> changes) const noexcept {
    Y_VERIFY(changes);
    Y_VERIFY(!changes->Blobs.empty());           // src data
    Y_VERIFY(!changes->PortionsToEvict.empty()); // src meta
    Y_VERIFY(changes->EvictedRecords.empty());   // dst meta

    std::vector<TString> newBlobs;
    std::vector<std::pair<TPortionInfo, TPortionEvictionFeatures>> evicted;
    evicted.reserve(changes->PortionsToEvict.size());

    for (auto& [portionInfo, evictFeatures] : changes->PortionsToEvict) {
        Y_VERIFY(!portionInfo.Empty());
        Y_VERIFY(portionInfo.IsActive());

        if (UpdateEvictedPortion(portionInfo, evictFeatures, changes->Blobs,
                                    changes->EvictedRecords, newBlobs)) {
            Y_VERIFY(portionInfo.TierName == evictFeatures.TargetTierName);
            evicted.emplace_back(std::move(portionInfo), evictFeatures);
        }
    }

    changes->PortionsToEvict.swap(evicted);
    return newBlobs;
}
}
