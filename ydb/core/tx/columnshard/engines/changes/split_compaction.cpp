#include "split_compaction.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/storage/granule.h>

namespace NKikimr::NOlap {

TConclusion<std::vector<TString>> TSplitCompactColumnEngineChanges::DoConstructBlobs(TConstructionContext& context) noexcept {
    const ui64 pathId = SrcGranule.PathId;
    const TMark ts0 = SrcGranule.Mark;
    std::vector<TPortionInfo>& portions = SwitchedPortions;

    std::vector<std::pair<TMark, ui64>> tsIds;
    ui64 movedRows = TryMovePortions(ts0, portions, tsIds, PortionsToMove, context);
    auto [srcBatches, maxSnapshot] = PortionsToBatches(portions, Blobs, movedRows != 0, context);
    Y_VERIFY(srcBatches.size() == portions.size());

    std::vector<TString> blobs;
    auto resultSchema = context.SchemaVersions.GetLastSchema();
    if (movedRows) {
        Y_VERIFY(PortionsToMove.size() >= 2);
        Y_VERIFY(PortionsToMove.size() == tsIds.size());
        Y_VERIFY(tsIds.begin()->first == ts0);

        // Calculate total number of rows.
        ui64 numRows = movedRows;
        for (const auto& batch : srcBatches) {
            numRows += batch->num_rows();
        }

        // Recalculate new granules borders (if they are larger then portions)
        ui32 numSplitInto = NumSplitInto(numRows);
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

                auto& toMove = PortionsToMove[i];
                sumRows += toMove.first.NumRows();
                toMove.second = tmpGranule;
            }

            tsIds.swap(newTsIds);
        }
        Y_VERIFY(tsIds.size() > 1);
        Y_VERIFY(tsIds[0] == std::make_pair(ts0, ui64(1)));
        TMarksGranules marksGranules(std::move(tsIds));

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
                PortionsToMove.emplace_back(std::move(portion), tmpGranule);
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
            ui64 tmpGranule = SetTmpGranule(pathId, mark);
            for (const auto& batch : idBatches[id]) {
                // Cannot set snapshot here. It would be set in committing transaction in ApplyChanges().
                auto newPortions = MakeAppendedPortions(pathId, batch, tmpGranule, maxSnapshot, blobs, GetGranuleMeta(), context);
                Y_VERIFY(newPortions.size() > 0);
                for (auto& portion : newPortions) {
                    AppendedPortions.emplace_back(std::move(portion));
                }
            }
        }
    } else {
        auto batches = SliceGranuleBatches(resultSchema->GetIndexInfo(), srcBatches, ts0);

        SetTmpGranule(pathId, ts0);
        for (auto& [ts, batch] : batches) {
            // Tmp granule would be updated to correct value in ApplyChanges()
            ui64 tmpGranule = SetTmpGranule(pathId, ts);

            // Cannot set snapshot here. It would be set in committing transaction in ApplyChanges().
            auto portions = MakeAppendedPortions(pathId, batch, tmpGranule, maxSnapshot, blobs, GetGranuleMeta(), context);
            Y_VERIFY(portions.size() > 0);
            for (auto& portion : portions) {
                AppendedPortions.emplace_back(std::move(portion));
            }
        }
    }

    return blobs;
}

std::pair<std::vector<std::shared_ptr<arrow::RecordBatch>>, NKikimr::NOlap::TSnapshot> TSplitCompactColumnEngineChanges::PortionsToBatches(
    const std::vector<TPortionInfo>& portions, const THashMap<TBlobRange, TString>& blobs, const bool insertedOnly, TConstructionContext& context) {
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    batches.reserve(portions.size());
    const auto resultSchema = context.SchemaVersions.GetLastSchema();
    TSnapshot maxSnapshot = resultSchema->GetSnapshot();
    for (const auto& portionInfo : portions) {
        if (!insertedOnly || portionInfo.IsInserted()) {
            const auto blobSchema = context.SchemaVersions.GetSchema(portionInfo.GetSnapshot());

            batches.push_back(portionInfo.AssembleInBatch(*blobSchema, *resultSchema, blobs));

            if (maxSnapshot < portionInfo.GetSnapshot()) {
                maxSnapshot = portionInfo.GetSnapshot();
            }
        }
    }
    return std::make_pair(std::move(batches), maxSnapshot);
}

std::vector<std::pair<NKikimr::NOlap::TMark, std::shared_ptr<arrow::RecordBatch>>> TSplitCompactColumnEngineChanges::SliceGranuleBatches(const TIndexInfo& indexInfo,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches, const TMark& ts0) {
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

        const auto effKey = TMarksGranules::GetEffectiveKey(batch, indexInfo);
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
    ui32 rowsInGranule = numRows / NumSplitInto(numRows);
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

        const auto effKey = TMarksGranules::GetEffectiveKey(batch, indexInfo);
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
            Y_VERIFY(offset <= batch->num_rows());
            batchOffsets.push_back(offset);
        }

        Y_VERIFY(batchOffsets.size() == borders.size() + 1);
    }

    // Make merge-sorted granule batch for each splitted granule
    for (ui32 granuleNo = 0; granuleNo < borders.size() + 1; ++granuleNo) {
        std::vector<std::shared_ptr<arrow::RecordBatch>> granuleBatches;
        granuleBatches.reserve(batches.size());
        const bool lastGranule = (granuleNo == borders.size());

        // Extract granule: slice source batches with offsets
        i64 granuleNumRows = 0;
        for (size_t i = 0; i < batches.size(); ++i) {
            const auto& batch = batches[i];
            auto& batchOffsets = offsets[i];

            int offset = batchOffsets[granuleNo];
            int end = lastGranule ? batch->num_rows() : batchOffsets[granuleNo + 1];
            int size = end - offset;
            Y_VERIFY(size >= 0);

            if (size) {
                Y_VERIFY(offset < batch->num_rows());
                auto slice = batch->Slice(offset, size);
                Y_VERIFY(slice->num_rows());
                granuleNumRows += slice->num_rows();
#if 1 // Check correctness
                const auto effKey = TMarksGranules::GetEffectiveKey(slice, indexInfo);
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
            const auto effKey = TMarksGranules::GetEffectiveKey(batch, indexInfo);
            Y_VERIFY(effKey->num_columns() && effKey->num_rows());

            Y_VERIFY(NArrow::TReplaceKey::FromBatch(effKey, 0) >= startKey);
#endif
            out.emplace_back(TMark(startKey), batch);
        }
    }

    return out;
}

ui64 TSplitCompactColumnEngineChanges::TryMovePortions(const TMark& ts0, std::vector<TPortionInfo>& portions,
    std::vector<std::pair<TMark, ui64>>& tsIds, std::vector<std::pair<TPortionInfo, ui64>>& toMove, TConstructionContext& context)
{
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

    context.Counters.AnalizeCompactedPortions->Add(compacted.size());
    context.Counters.AnalizeInsertedPortions->Add(inserted.size());
    for (auto&& i : portions) {
        if (i.IsInserted()) {
            context.Counters.AnalizeInsertedBytes->Add(i.BlobsBytes());
        } else {
            context.Counters.AnalizeCompactedBytes->Add(i.BlobsBytes());
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
        context.Counters.RepackedInsertedPortionBytes->Add(i->BlobsBytes());
    }
    context.Counters.RepackedInsertedPortions->Add(inserted.size());

    // Check that there are no gaps between two adjacent portions in term of primary key range.
    for (size_t i = 0; i < compacted.size() - 1; ++i) {
        if (compacted[i]->IndexKeyEnd() >= compacted[i + 1]->IndexKeyStart()) {
            for (auto&& c : compacted) {
                context.Counters.SkipPortionBytesMoveThroughIntersection->Add(c->BlobsBytes());
            }

            context.Counters.SkipPortionsMoveThroughIntersection->Add(compacted.size());
            context.Counters.RepackedCompactedPortions->Add(compacted.size());
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
        context.Counters.MovedPortionBytes->Add(portionInfo->BlobsBytes());
        tsIds.emplace_back((counter ? TMark(portionInfo->IndexKeyStart()) : ts0), counter + 1);
        toMove.emplace_back(std::move(*portionInfo), counter);
        ++counter;
        // Ensure that std::move will take an effect.
        static_assert(std::swappable<decltype(*portionInfo)>);
    }
    context.Counters.MovedPortions->Add(toMove.size());

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

void TSplitCompactColumnEngineChanges::DoWriteIndexComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context) {
    TBase::DoWriteIndexComplete(self, context);
    self.IncCounter(context.FinishedSuccessfully ? NColumnShard::COUNTER_SPLIT_COMPACTION_SUCCESS : NColumnShard::COUNTER_SPLIT_COMPACTION_FAIL);
    self.IncCounter(NColumnShard::COUNTER_SPLIT_COMPACTION_BLOBS_WRITTEN, context.BlobsWritten);
    self.IncCounter(NColumnShard::COUNTER_SPLIT_COMPACTION_BYTES_WRITTEN, context.BytesWritten);
}

void TSplitCompactColumnEngineChanges::DoStart(NColumnShard::TColumnShard& self) {
    TBase::DoStart(self);
    auto& g = CompactionInfo->GetObject<TGranuleMeta>();
    self.CSCounters.OnSplitCompactionInfo(g.GetAdditiveSummary().GetOther().GetPortionsSize(), g.GetAdditiveSummary().GetOther().GetPortionsCount());
}

}
