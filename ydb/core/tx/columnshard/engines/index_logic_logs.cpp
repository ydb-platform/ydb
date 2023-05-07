#include "index_logic_logs.h"

#include <span>

namespace NKikimr::NOlap {

std::shared_ptr<arrow::RecordBatch> TIndexLogicBase::GetEffectiveKey(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                        const TIndexInfo& indexInfo) {
    // TODO: composite effective key
    auto columnName = indexInfo.GetPrimaryKey()[0].first;
    auto resBatch = NArrow::ExtractColumns(batch, {std::string(columnName.data(), columnName.size())});
    Y_VERIFY_S(resBatch, "No column '" << columnName << "' in batch " << batch->schema()->ToString());
    return resBatch;
}

arrow::ipc::IpcWriteOptions WriteOptions(const TCompression& compression) {
    auto& codec = compression.Codec;

    arrow::ipc::IpcWriteOptions options(arrow::ipc::IpcWriteOptions::Defaults());
    Y_VERIFY(arrow::util::Codec::IsAvailable(codec));
    arrow::Result<std::unique_ptr<arrow::util::Codec>> resCodec;
    if (compression.Level) {
        resCodec = arrow::util::Codec::Create(codec, *compression.Level);
        if (!resCodec.ok()) {
            resCodec = arrow::util::Codec::Create(codec);
        }
    } else {
        resCodec = arrow::util::Codec::Create(codec);
    }
    Y_VERIFY(resCodec.ok());

    options.codec.reset((*resCodec).release());
    options.use_threads = false;
    return options;
}

std::shared_ptr<arrow::RecordBatch> TIndexationLogic::AddSpecials(const std::shared_ptr<arrow::RecordBatch>& srcBatch,
                                                const TIndexInfo& indexInfo, const TInsertedData& inserted) const {
    auto batch = TIndexInfo::AddSpecialColumns(srcBatch, inserted.PlanStep(), inserted.TxId());
    Y_VERIFY(batch);

    return NArrow::ExtractColumns(batch, indexInfo.ArrowSchemaWithSpecials());
}

bool TEvictionLogic::UpdateEvictedPortion(TPortionInfo& portionInfo,
                                            TPortionEvictionFeatures& evictFeatures, const THashMap<TBlobRange, TString>& srcBlobs,
                                            TVector<TColumnRecord>& evictedRecords, TVector<TString>& newBlobs) const {
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

    auto schema = IndexInfo.ArrowSchemaWithSpecials();
    auto batch = portionInfo.AssembleInBatch(IndexInfo, schema, srcBlobs);
    auto writeOptions = WriteOptions(*compression);

    TPortionInfo undo = portionInfo;
    size_t undoSize = newBlobs.size();

    for (auto& rec : portionInfo.Records) {
        auto colName = IndexInfo.GetColumnName(rec.ColumnId);
        std::string name(colName.data(), colName.size());
        auto field = schema->GetFieldByName(name);

        auto blob = TPortionInfo::SerializeColumn(batch->GetColumnByName(name), field, writeOptions);
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

    portionInfo.AddMetadata(IndexInfo, batch, evictFeatures.TargetTierName);
    return true;
}

TVector<TPortionInfo> TIndexLogicBase::MakeAppendedPortions(const ui64 pathId,
                                                            const std::shared_ptr<arrow::RecordBatch> batch,
                                                            const ui64 granule,
                                                            const TSnapshot& minSnapshot,
                                                            TVector<TString>& blobs) const {
    Y_VERIFY(batch->num_rows());
    const auto schema = IndexInfo.ArrowSchemaWithSpecials();
    TVector<TPortionInfo> out;

    TString tierName;
    TCompression compression = IndexInfo.GetDefaultCompression();
    if (pathId) {
        if (auto* tiering = GetTieringMap().FindPtr(pathId)) {
            tierName = tiering->GetHottestTierName();
            if (const auto& tierCompression = tiering->GetCompression(tierName)) {
                compression = *tierCompression;
            }
        }
    }
    const auto writeOptions = WriteOptions(compression);

    std::shared_ptr<arrow::RecordBatch> portionBatch = batch;
    for (i32 pos = 0; pos < batch->num_rows();) {
        Y_VERIFY(portionBatch->num_rows());

        TPortionInfo portionInfo;
        portionInfo.Records.reserve(schema->num_fields());
        TVector<TString> portionBlobs;
        portionBlobs.reserve(schema->num_fields());

        // Serialize portion's columns into blobs

        bool ok = true;
        for (const auto& field : schema->fields()) {
            const auto& name = field->name();
            ui32 columnId = IndexInfo.GetColumnId(TString(name.data(), name.size()));

            /// @warnign records are not valid cause of empty BlobId and zero Portion
            TColumnRecord record = TColumnRecord::Make(granule, columnId, minSnapshot, 0);
            auto blob = portionInfo.AddOneChunkColumn(portionBatch->GetColumnByName(name), field, std::move(record),
                                                        writeOptions);
            if (!blob.size()) {
                ok = false;
                break;
            }

            // TODO: combine small columns in one blob
            portionBlobs.emplace_back(std::move(blob));
        }

        if (ok) {
            portionInfo.AddMetadata(IndexInfo, portionBatch, tierName);
            out.emplace_back(std::move(portionInfo));
            for (auto& blob : portionBlobs) {
                blobs.push_back(blob);
            }
            pos += portionBatch->num_rows();
            if (pos < batch->num_rows()) {
                portionBatch = batch->Slice(pos);
            }
        } else {
            const i64 halfLen = portionBatch->num_rows() / 2;
            Y_VERIFY(halfLen);
            portionBatch = batch->Slice(pos, halfLen);
        }
    }

    return out;
}

std::vector<std::shared_ptr<arrow::RecordBatch>> TCompactionLogic::PortionsToBatches(const TVector<TPortionInfo>& portions,
                                                                                        const THashMap<TBlobRange, TString>& blobs,
                                                                                        bool insertedOnly) const {
    // TODO: schema changes
    const std::shared_ptr<arrow::Schema> schema = IndexInfo.ArrowSchemaWithSpecials();

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    batches.reserve(portions.size());

    for (auto& portionInfo : portions) {
        auto batch = portionInfo.AssembleInBatch(IndexInfo, schema, blobs);
        if (!insertedOnly || portionInfo.IsInserted()) {
            batches.push_back(batch);
        }
    }
    return batches;
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
                                : NArrow::LowerBound(keys, granules[i + 1].first.Border, offset);

            if (const i64 size = end - offset) {
                Y_VERIFY(out.emplace(granules[i].second, batch->Slice(offset, size)).second);
            }

            offset = end;
        }
    }
    return out;
}

TVector<TString> TIndexationLogic::Apply(std::shared_ptr<TColumnEngineChanges> indexChanges) const {
    auto changes = std::static_pointer_cast<TColumnEngineForLogs::TChanges>(indexChanges);
    Y_VERIFY(!changes->DataToIndex.empty());
    Y_VERIFY(changes->AppendedPortions.empty());
    Y_VERIFY(IndexInfo.IsSorted());

    TSnapshot& minSnapshot = changes->ApplySnapshot;
    THashMap<ui64, std::vector<std::shared_ptr<arrow::RecordBatch>>> pathBatches;
    for (auto& inserted : changes->DataToIndex) {
        TSnapshot insertSnap{inserted.PlanStep(), inserted.TxId()};
        Y_VERIFY(insertSnap.Valid());
        if (minSnapshot.IsZero() || insertSnap <= minSnapshot) {
            minSnapshot = insertSnap;
        }

        TBlobRange blobRange(inserted.BlobId, 0, inserted.BlobId.BlobSize());

        std::shared_ptr<arrow::RecordBatch> batch;
        if (auto it = changes->CachedBlobs.find(inserted.BlobId); it != changes->CachedBlobs.end()) {
            batch = it->second;
        } else if (auto* blobData = changes->Blobs.FindPtr(blobRange)) {
            Y_VERIFY(!blobData->empty(), "Blob data not present");
            batch = NArrow::DeserializeBatch(*blobData, IndexInfo.ArrowSchema());
        } else {
            Y_VERIFY(blobData, "Data for range %s has not been read", blobRange.ToString().c_str());
        }
        Y_VERIFY(batch);

        batch = AddSpecials(batch, IndexInfo, inserted);
        pathBatches[inserted.PathId].push_back(batch);
        Y_VERIFY_DEBUG(NArrow::IsSorted(pathBatches[inserted.PathId].back(), IndexInfo.GetReplaceKey()));
    }
    Y_VERIFY(minSnapshot.Valid());

    TVector<TString> blobs;

    for (auto& [pathId, batches] : pathBatches) {
        changes->AddPathIfNotExists(pathId);

        // We could merge data here cause tablet limits indexing data portions
#if 0
    auto merged = NArrow::CombineSortedBatches(batches, indexInfo.SortDescription()); // insert: no replace
    Y_VERIFY(merged);
    Y_VERIFY_DEBUG(NArrow::IsSorted(merged, indexInfo.GetReplaceKey()));
#else
        auto merged = NArrow::CombineSortedBatches(batches, IndexInfo.SortReplaceDescription());
        Y_VERIFY(merged);
        Y_VERIFY_DEBUG(NArrow::IsSortedAndUnique(merged, IndexInfo.GetReplaceKey()));

#endif

        auto granuleBatches = SliceIntoGranules(merged, changes->PathToGranule[pathId], IndexInfo);
        for (auto& [granule, batch] : granuleBatches) {
            auto portions = MakeAppendedPortions(pathId, batch, granule, minSnapshot, blobs);
            Y_VERIFY(portions.size() > 0);
            for (auto& portion : portions) {
                changes->AppendedPortions.emplace_back(std::move(portion));
            }
        }
    }

    Y_VERIFY(changes->PathToGranule.size() == pathBatches.size());
    return blobs;
}

std::shared_ptr<arrow::RecordBatch> TCompactionLogic::CompactInOneGranule(ui64 granule,
                                                                            const TVector<TPortionInfo>& portions,
                                                                            const THashMap<TBlobRange, TString>& blobs) const {
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    batches.reserve(portions.size());

    auto schema = IndexInfo.ArrowSchemaWithSpecials();
    for (auto& portionInfo : portions) {
        Y_VERIFY(!portionInfo.Empty());
        Y_VERIFY(portionInfo.Granule() == granule);

        auto batch = portionInfo.AssembleInBatch(IndexInfo, schema, blobs);
        batches.push_back(batch);
    }

    auto sortedBatch = NArrow::CombineSortedBatches(batches, IndexInfo.SortReplaceDescription());
    Y_VERIFY_DEBUG(NArrow::IsSortedAndUnique(sortedBatch, IndexInfo.GetReplaceKey()));

    return sortedBatch;
}

TVector<TString> TCompactionLogic::CompactInGranule(std::shared_ptr<TColumnEngineForLogs::TChanges> changes) const {
    const ui64 pathId = changes->SrcGranule->PathId;
    TVector<TString> blobs;
    auto& switchedProtions = changes->SwitchedPortions;
    Y_VERIFY(switchedProtions.size());

    ui64 granule = switchedProtions[0].Granule();
    auto batch = CompactInOneGranule(granule, switchedProtions, changes->Blobs);

    TVector<TPortionInfo> portions;
    if (!changes->MergeBorders.Empty()) {
        Y_VERIFY(changes->MergeBorders.GetOrderedMarks().size() > 1);
        auto slices = changes->MergeBorders.SliceIntoGranules(batch, IndexInfo);
        portions.reserve(slices.size());

        for (auto& [_, slice] : slices) {
            if (!slice || slice->num_rows() == 0) {
                continue;
            }
            auto tmp = MakeAppendedPortions(pathId, slice, granule, TSnapshot{}, blobs);
            for (auto&& portionInfo : tmp) {
                portions.emplace_back(std::move(portionInfo));
            }
        }
    } else {
        portions = MakeAppendedPortions(pathId, batch, granule, TSnapshot{}, blobs);
    }

    Y_VERIFY(portions.size() > 0);
    for (auto& portion : portions) {
        changes->AppendedPortions.emplace_back(std::move(portion));
    }

    return blobs;
}

TVector<std::pair<TMark, std::shared_ptr<arrow::RecordBatch>>>
TCompactionLogic::SliceGranuleBatches(const TIndexInfo& indexInfo,
                                        const TColumnEngineForLogs::TChanges& changes,
                                        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
                                        const TMark& ts0) const {
    TVector<std::pair<TMark, std::shared_ptr<arrow::RecordBatch>>> out;

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
    Y_VERIFY(minTs >= ts0.Border);

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
    TVector<NArrow::TReplaceKey> borders;
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
    TVector<TVector<int>> offsets(batches.size()); // vec[batch][border] = offset
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

            auto startKey = ts0.Border;
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
                                        TVector<TPortionInfo>& portions,
                                        std::vector<std::pair<TMark, ui64>>& tsIds,
                                        TVector<std::pair<TPortionInfo, ui64>>& toMove) const {
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

    // Do nothing if there are less than two compacted protions.
    if (compacted.size() < 2) {
        return 0;
    }
    // Order compacted portions by primary key.
    std::sort(compacted.begin(), compacted.end(), [](const TPortionInfo* a, const TPortionInfo* b) {
        return a->EffKeyStart() < b->EffKeyStart();
    });
    // Check that there are no gaps between two adjacent portions in term of primary key range.
    for (size_t i = 0; i < compacted.size() - 1; ++i) {
        if (compacted[i]->EffKeyEnd() >= compacted[i + 1]->EffKeyStart()) {
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
        tsIds.emplace_back((counter ? TMark(portionInfo->EffKeyStart()) : ts0), counter + 1);
        toMove.emplace_back(std::move(*portionInfo), counter);
        ++counter;
        // Ensure that std::move will take an effect.
        static_assert(std::swappable<decltype(*portionInfo)>);
    }

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

TVector<TString> TCompactionLogic::CompactSplitGranule(const std::shared_ptr<TColumnEngineForLogs::TChanges>& changes) const {
    const ui64 pathId = changes->SrcGranule->PathId;
    const TMark ts0 = changes->SrcGranule->Mark;
    TVector<TPortionInfo>& portions = changes->SwitchedPortions;

    std::vector<std::pair<TMark, ui64>> tsIds;
    ui64 movedRows = TryMovePortions(ts0, portions, tsIds, changes->PortionsToMove);
    const auto& srcBatches = PortionsToBatches(portions, changes->Blobs, movedRows != 0);
    Y_VERIFY(srcBatches.size() == portions.size());

    TVector<TString> blobs;

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

            TVector<std::pair<TMark, ui64>> newTsIds;
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
        TVector<TPortionInfo*> toSwitch;
        toSwitch.reserve(portions.size());
        for (size_t i = 0; i < portions.size(); ++i) {
            auto& portion = portions[i];
            auto& batch = srcBatches[i];
            auto slices = marksGranules.SliceIntoGranules(batch, IndexInfo);

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
            TVector<TPortionInfo> tmp;
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
                auto newPortions = MakeAppendedPortions(pathId, batch, tmpGranule, TSnapshot{}, blobs);
                Y_VERIFY(newPortions.size() > 0);
                for (auto& portion : newPortions) {
                    changes->AppendedPortions.emplace_back(std::move(portion));
                }
            }
        }
    } else {
        auto batches = SliceGranuleBatches(IndexInfo, *changes, srcBatches, ts0);

        changes->SetTmpGranule(pathId, ts0);
        for (auto& [ts, batch] : batches) {
            // Tmp granule would be updated to correct value in ApplyChanges()
            ui64 tmpGranule = changes->SetTmpGranule(pathId, ts);

            // Cannot set snapshot here. It would be set in committing transaction in ApplyChanges().
            auto portions = MakeAppendedPortions(pathId, batch, tmpGranule, TSnapshot{}, blobs);
            Y_VERIFY(portions.size() > 0);
            for (auto& portion : portions) {
                changes->AppendedPortions.emplace_back(std::move(portion));
            }
        }
    }

    return blobs;
}

TVector<TString> TCompactionLogic::Apply(std::shared_ptr<TColumnEngineChanges> changes) const {
    Y_VERIFY(changes);
    Y_VERIFY(changes->CompactionInfo);
    Y_VERIFY(changes->DataToIndex.empty());       // not used
    Y_VERIFY(!changes->Blobs.empty());            // src data
    Y_VERIFY(!changes->SwitchedPortions.empty()); // src meta
    Y_VERIFY(changes->AppendedPortions.empty());  // dst meta

    auto castedChanges = std::static_pointer_cast<TColumnEngineForLogs::TChanges>(changes);
    if (castedChanges->CompactionInfo->InGranule) {
        return CompactInGranule(castedChanges);
    }
    return CompactSplitGranule(castedChanges);
}

TVector<TString> TEvictionLogic::Apply(std::shared_ptr<TColumnEngineChanges> changes) const {
    Y_VERIFY(changes);
    Y_VERIFY(!changes->Blobs.empty());           // src data
    Y_VERIFY(!changes->PortionsToEvict.empty()); // src meta
    Y_VERIFY(changes->EvictedRecords.empty());   // dst meta

    TVector<TString> newBlobs;
    TVector<std::pair<TPortionInfo, TPortionEvictionFeatures>> evicted;
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
