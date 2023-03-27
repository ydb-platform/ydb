#include "defs.h"
#include "indexed_read_data.h"
#include "filter.h"
#include "column_engine_logs.h"
#include <ydb/core/tx/columnshard/columnshard__index_scan.h>
#include <ydb/core/tx/columnshard/columnshard__stats_scan.h>
#include <ydb/core/formats/one_batch_input_stream.h>
#include <ydb/core/formats/merging_sorted_input_stream.h>
#include <ydb/core/formats/custom_registry.h>

namespace NKikimr::NOlap {

namespace {

using TMark = TColumnEngineForLogs::TMark;

// Slices a batch into smaller batches and appends them to result vector (which might be non-empty already)
void SliceBatch(const std::shared_ptr<arrow::RecordBatch>& batch,
                const int64_t maxRowsInBatch,
                std::vector<std::shared_ptr<arrow::RecordBatch>>& result)
{
    if (batch->num_rows() <= maxRowsInBatch) {
        result.push_back(batch);
        return;
    }

    int64_t offset = 0;
    while (offset < batch->num_rows()) {
        int64_t rows = std::min<int64_t>(maxRowsInBatch, batch->num_rows() - offset);
        result.emplace_back(batch->Slice(offset, rows));
        offset += rows;
    }
};

std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>>
GroupInKeyRanges(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches, const TIndexInfo& indexInfo) {
    std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>> rangesSlices; // rangesSlices[rangeNo][sliceNo]
    rangesSlices.reserve(batches.size());
    {
        TMap<TMark, std::vector<std::shared_ptr<arrow::RecordBatch>>> points;

        for (auto& batch : batches) {
            std::shared_ptr<arrow::Array> keyColumn = GetFirstPKColumn(indexInfo, batch);
            Y_VERIFY(keyColumn && keyColumn->length() > 0);

            TMark min(*keyColumn->GetScalar(0));
            TMark max(*keyColumn->GetScalar(keyColumn->length() - 1));

            points[min].push_back(batch); // insert start
            points[max].push_back({}); // insert end
        }

        int sum = 0;
        for (auto& [key, vec] : points) {
            if (!sum) { // count(start) == count(end), start new range
                rangesSlices.push_back({});
                rangesSlices.back().reserve(batches.size());
            }

            for (auto& batch : vec) {
                if (batch) {
                    ++sum;
                    rangesSlices.back().push_back(batch);
                } else {
                    --sum;
                }
            }
        }
    }
    return rangesSlices;
}

std::vector<std::shared_ptr<arrow::RecordBatch>> SpecialMergeSorted(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
                                                                    const TIndexInfo& indexInfo,
                                                                    const std::shared_ptr<NArrow::TSortDescription>& description,
                                                                    const THashSet<const void*> batchesToDedup) {
    auto rangesSlices = GroupInKeyRanges(batches, indexInfo);

    // Merge slices in ranges
    std::vector<std::shared_ptr<arrow::RecordBatch>> out;
    out.reserve(rangesSlices.size());
    for (auto& slices : rangesSlices) {
        if (slices.empty()) {
            continue;
        }

        // Do not merge slice if it's alone in its key range
        if (slices.size() == 1) {
            auto batch = slices[0];
            if (batchesToDedup.count(batch.get())) {
                NArrow::DedupSortedBatch(batch, description->ReplaceKey, out);
            } else {
                Y_VERIFY_DEBUG(NArrow::IsSortedAndUnique(batch, description->ReplaceKey));
                out.push_back(batch);
            }
            continue;
        }
#if 0 // optimization
        auto deduped = SliceSortedBatches(slices, description);
        for (auto& batch : deduped) {
            if (batch && batch->num_rows()) {
                Y_VERIFY_DEBUG(NArrow::IsSortedAndUnique(batch, description->ReplaceKey));
                out.push_back(batch);
            }
        }
#else
        auto batch = NArrow::CombineSortedBatches(slices, description);
        out.push_back(batch);
#endif
    }

    return out;
}

}

std::unique_ptr<NColumnShard::TScanIteratorBase> TReadMetadata::StartScan() const {
    return std::make_unique<NColumnShard::TColumnShardScanIterator>(this->shared_from_this());
}


TVector<std::pair<TString, NScheme::TTypeInfo>> TReadStatsMetadata::GetResultYqlSchema() const {
    return NOlap::GetColumns(NColumnShard::PrimaryIndexStatsSchema, ResultColumnIds);
}

TVector<std::pair<TString, NScheme::TTypeInfo>> TReadStatsMetadata::GetKeyYqlSchema() const {
    return NOlap::GetColumns(NColumnShard::PrimaryIndexStatsSchema, NColumnShard::PrimaryIndexStatsSchema.KeyColumns);
}

std::unique_ptr<NColumnShard::TScanIteratorBase> TReadStatsMetadata::StartScan() const {
    return std::make_unique<NColumnShard::TStatsIterator>(this->shared_from_this());
}


THashMap<TBlobRange, ui64> TIndexedReadData::InitRead(ui32 inputBatch, bool inGranulesOrder) {
    Y_VERIFY(ReadMetadata->BlobSchema);
    Y_VERIFY(ReadMetadata->LoadSchema);
    Y_VERIFY(ReadMetadata->ResultSchema);
    Y_VERIFY(IndexInfo().GetSortingKey());
    Y_VERIFY(IndexInfo().GetIndexKey() && IndexInfo().GetIndexKey()->num_fields());

    SortReplaceDescription = IndexInfo().SortReplaceDescription();

    NotIndexed.resize(inputBatch);
    FirstIndexedBatch = inputBatch;

    ui32 batchNo = inputBatch;
    BatchPortion.resize(inputBatch + ReadMetadata->SelectInfo->Portions.size());
    THashMap<TBlobRange, ui64> out;

    ui64 dataBytes = 0;
    for (auto& portionInfo : ReadMetadata->SelectInfo->Portions) {
        Y_VERIFY_S(portionInfo.Records.size() > 0, "ReadMeatadata: " << *ReadMetadata);

        ui64 portion = portionInfo.Records[0].Portion;
        ui64 granule = portionInfo.Records[0].Granule;
        PortionBatch[portion] = batchNo;
        BatchPortion[batchNo] = portion;
        PortionGranule[portion] = granule;
        if (!GranuleWaits.count(granule)) {
            GranuleWaits[granule] = 1;
        } else {
            ++GranuleWaits[granule];
        }

        // If there's no PK dups in granule we could use optimized version of merge
        if (portionInfo.CanIntersectOthers()) {
            GranulesWithDups.emplace(granule);
            if (portionInfo.CanHaveDups()) {
                PortionsWithDups.emplace(portion);
            }
        }

        for (const NOlap::TColumnRecord& rec : portionInfo.Records) {
            WaitIndexed[batchNo].insert(rec.BlobRange);
            IndexedBlobs[rec.BlobRange] = batchNo;
            out[rec.BlobRange] = rec.Granule;
            dataBytes += rec.BlobRange.Size;

            Y_VERIFY_S(rec.Valid(), "ReadMeatadata: " << *ReadMetadata);
            Y_VERIFY_S(PortionGranule[rec.Portion] == rec.Granule, "ReadMeatadata: " << *ReadMetadata);
        }

        ++batchNo;
    }

    if (inGranulesOrder) {
        for (auto& granuleInfo : ReadMetadata->SelectInfo->Granules) {
            ui64 granule = granuleInfo.Granule;
            Y_VERIFY_S(GranuleWaits.count(granule), "ReadMeatadata: " << *ReadMetadata);
            if (ReadMetadata->IsAscSorted()) {
                GranulesOutOrder.push_back(granule);
            } else if (ReadMetadata->IsDescSorted()) {
                GranulesOutOrder.push_front(granule);
            }
        }
    }

    // Init split by granules structs
    for (auto& rec : ReadMetadata->SelectInfo->Granules) {
        TsGranules.emplace(rec.Mark, rec.Granule);
    }

    TMark minMark(IndexInfo().GetIndexKey()->field(0)->type());
    if (!TsGranules.count(minMark)) {
        // committed data before the first granule would be placed in fake (0,0) granule
        // committed data after the last granule would be placed into the last granule (or here if none)
        TsGranules.emplace(minMark, 0);
    }

    auto& stats = ReadMetadata->ReadStats;
    stats->IndexGranules = GranuleWaits.size();
    stats->IndexPortions = PortionGranule.size();
    stats->IndexBatches = ReadMetadata->NumIndexedBlobs();
    stats->CommittedBatches = ReadMetadata->CommittedBlobs.size();
    stats->UsedColumns = ReadMetadata->LoadSchema->num_fields();
    stats->DataBytes = dataBytes;
    return out;
}

void TIndexedReadData::AddIndexed(const TBlobRange& blobRange, const TString& column) {
    Y_VERIFY(IndexedBlobs.count(blobRange));
    ui32 batchNo = IndexedBlobs[blobRange];
    if (!WaitIndexed.count(batchNo)) {
        return;
    }
    auto& waitingFor = WaitIndexed[batchNo];
    waitingFor.erase(blobRange);

    Data[blobRange] = column;

    if (waitingFor.empty()) {
        WaitIndexed.erase(batchNo);
        if (auto batch = AssembleIndexedBatch(batchNo)) {
            Indexed[batchNo] = batch;
        }
        UpdateGranuleWaits(batchNo);
    }
}

std::shared_ptr<arrow::RecordBatch> TIndexedReadData::AssembleIndexedBatch(ui32 batchNo) {
    auto& portionInfo = Portion(batchNo);
    Y_VERIFY(portionInfo.Produced());

    auto batch = portionInfo.AssembleInBatch(ReadMetadata->IndexInfo, ReadMetadata->LoadSchema, Data);
    Y_VERIFY(batch);

    for (auto& rec : portionInfo.Records) {
        auto& blobRange = rec.BlobRange;
        Data.erase(blobRange);
    }

    /// @warning The replace logic is correct only in assumption that predicate is applyed over a part of ReplaceKey.
    /// It's not OK to apply predicate before replacing key duplicates otherwise.
    /// Assumption: dup(A, B) <=> PK(A) = PK(B) => Predicate(A) = Predicate(B) => all or no dups for PK(A) here
    auto filtered = NOlap::FilterPortion(batch, *ReadMetadata);
    if (filtered.Batch) {
        Y_VERIFY(filtered.Valid());
        filtered.ApplyFilter();
    }
#if 1 // optimization
    if (filtered.Batch && ReadMetadata->Program && portionInfo.AllowEarlyFilter()) {
        filtered = NOlap::EarlyFilter(filtered.Batch, ReadMetadata->Program);
    }
    if (filtered.Batch) {
        Y_VERIFY(filtered.Valid());
        filtered.ApplyFilter();
    }
#endif
    return filtered.Batch;
}

void TIndexedReadData::UpdateGranuleWaits(ui32 batchNo) {
    ui64 granule = BatchGranule(batchNo);
    if (GranuleWaits.count(granule)) {
        ui32& count = GranuleWaits[granule];
        --count;
        if (!count) {
            ReadyGranules[granule] = {};
            GranuleWaits.erase(granule);
        }
    }
}

std::shared_ptr<arrow::RecordBatch>
TIndexedReadData::MakeNotIndexedBatch(const std::shared_ptr<arrow::RecordBatch>& srcBatch, ui64 planStep, ui64 txId) const {
    Y_VERIFY(srcBatch);

    // Extract columns (without check), filter, attach snapshot, extract columns with check
    // (do not filter snapshot columns)

    auto batch = NArrow::ExtractExistedColumns(srcBatch, ReadMetadata->LoadSchema);
    Y_VERIFY(batch);

    auto filtered = FilterNotIndexed(batch, *ReadMetadata);
    if (!filtered.Batch) {
        return {};
    }

    filtered.Batch = TIndexInfo::AddSpecialColumns(filtered.Batch, planStep, txId);
    Y_VERIFY(filtered.Batch);

    filtered.Batch = NArrow::ExtractColumns(filtered.Batch, ReadMetadata->LoadSchema);
    Y_VERIFY(filtered.Batch);

    Y_VERIFY(filtered.Valid());
    filtered.ApplyFilter();
    return filtered.Batch;
}

TVector<TPartialReadResult> TIndexedReadData::GetReadyResults(const int64_t maxRowsInBatch) {
    Y_VERIFY(SortReplaceDescription);

    if (NotIndexed.size() != ReadyNotIndexed) {
        // Wait till we have all not indexed data so we could replace keys in granules
        return {};
    }

    // First time extract OutNotIndexed data
    if (NotIndexed.size()) {
        /// @note not indexed data could contain data out of indexed granules
        Y_VERIFY(!TsGranules.empty());
        auto mergedBatch = MergeNotIndexed(std::move(NotIndexed)); // merged has no dups
        if (mergedBatch) {
            OutNotIndexed = SliceIntoGranules(mergedBatch, TsGranules, IndexInfo());
        }
        NotIndexed.clear();
        ReadyNotIndexed = 0;
    }

    // Extact ready granules (they are ready themselves but probably not ready to go out)
    std::vector<ui32> ready;
    for (auto& [batchNo, batch] : Indexed) {
        ui64 granule = BatchGranule(batchNo);
        if (ReadyGranules.count(granule)) {
            Y_VERIFY(batch);
            if (batch->num_rows()) {
                ui64 portion = BatchPortion[batchNo];
                if (PortionsWithDups.count(portion)) {
                    Y_VERIFY(GranulesWithDups.count(granule));
                    BatchesToDedup.insert(batch.get());
                } else {
                    Y_VERIFY_DEBUG(NArrow::IsSortedAndUnique(batch, IndexInfo().GetReplaceKey(), false));
                }
                ReadyGranules[granule].push_back(batch);
            }
            ready.push_back(batchNo);
        }
    }

    // Remove ready batches from future extraction
    for (ui32 batchNo : ready) {
        Indexed.erase(batchNo);
    }

    // Extract ready to out granules: ready granules that are not blocked by other (not ready) granules
    bool requireResult = !HasIndexRead(); // not indexed or the last indexed read (even if it's emply)
    auto out = MakeResult(ReadyToOut(), maxRowsInBatch);
    if (requireResult && out.empty()) {
        out.push_back(TPartialReadResult{
            .ResultBatch = NArrow::MakeEmptyBatch(ReadMetadata->ResultSchema)
        });
    }
    return out;
}

template <typename TCont>
static std::vector<ui64> GetReadyInOrder(const TCont& ready, TDeque<ui64>& order) {
    std::vector<ui64> out;
    out.reserve(ready.size());

    if (order.empty()) {
        for (auto& [granule, _] : ready) {
            out.push_back(granule);
        }
    } else {
        while (order.size()) {
            ui64 granule = order.front();
            if (!ready.count(granule)) {
                break;
            }
            out.push_back(granule);
            order.pop_front();
        }
    }

    return out;
}

/// @return batches that are not blocked by others
std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>> TIndexedReadData::ReadyToOut() {
    Y_VERIFY(SortReplaceDescription);

    std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>> out;
    out.reserve(ReadyGranules.size() + 1);

    // Prepend not indexed data (less then first granule) before granules for ASC sorting
    if (ReadMetadata->IsAscSorted() && OutNotIndexed.count(0)) {
        out.push_back({});
        out.back().push_back(OutNotIndexed[0]);
        OutNotIndexed.erase(0);
    }

    std::vector<ui64> ready = GetReadyInOrder(ReadyGranules, GranulesOutOrder);
    for (ui64 granule : ready) {
        std::vector<std::shared_ptr<arrow::RecordBatch>> inGranule = std::move(ReadyGranules[granule]);
        ReadyGranules.erase(granule);
        bool canHaveDups = GranulesWithDups.count(granule);

        // Append not indexed data to granules
        if (OutNotIndexed.count(granule)) {
            auto batch = OutNotIndexed[granule];
            if (batch && batch->num_rows()) { // TODO: check why it could be empty
                inGranule.push_back(batch);
                canHaveDups = true;
            }
            OutNotIndexed.erase(granule);
        }

        if (inGranule.empty()) {
            continue;
        }

        if (canHaveDups) {
            for (auto& batch : inGranule) {
                Y_VERIFY(batch->num_rows());
                Y_VERIFY_DEBUG(NArrow::IsSorted(batch, SortReplaceDescription->ReplaceKey));
            }
#if 1 // optimization
            auto deduped = SpecialMergeSorted(inGranule, IndexInfo(), SortReplaceDescription, BatchesToDedup);
            out.emplace_back(std::move(deduped));
#else
            out.push_back({});
            out.back().emplace_back(CombineSortedBatches(inGranule, SortReplaceDescription));
#endif
        } else {
            out.emplace_back(std::move(inGranule));
        }
    }

    // Append not indexed data (less then first granule) after granules for DESC sorting
    if (ReadMetadata->IsDescSorted() && GranulesOutOrder.empty() && OutNotIndexed.count(0)) {
        out.push_back({});
        out.back().push_back(OutNotIndexed[0]);
        OutNotIndexed.erase(0);
    }

    return out;
}

std::shared_ptr<arrow::RecordBatch>
TIndexedReadData::MergeNotIndexed(std::vector<std::shared_ptr<arrow::RecordBatch>>&& batches) const {
    Y_VERIFY(ReadMetadata->IsSorted());
    Y_VERIFY(IndexInfo().GetSortingKey());

    { // remove empty batches
        size_t dst = 0;
        for (size_t src = 0; src < batches.size(); ++src) {
            if (batches[src] && batches[src]->num_rows()) {
                if (dst != src) {
                    batches[dst] = batches[src];
                }
                ++dst;
            }
        }
        batches.resize(dst);
    }

    if (batches.empty()) {
        return {};
    }

    // We could merge data here only if backpressure limits committed data size. KIKIMR-12520
    auto& indexInfo = IndexInfo();
    auto merged = NArrow::CombineSortedBatches(batches, indexInfo.SortReplaceDescription());
    Y_VERIFY(merged);
    Y_VERIFY_DEBUG(NArrow::IsSortedAndUnique(merged, indexInfo.GetReplaceKey()));
    return merged;
}

// TODO: better implementation
static void MergeTooSmallBatches(TVector<TPartialReadResult>& out) {
    if (out.size() < 10) {
        return;
    }

    i64 sumRows = 0;
    for (auto& result : out) {
        sumRows += result.ResultBatch->num_rows();
    }
    if (sumRows / out.size() > 100) {
        return;
    }

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    batches.reserve(out.size());
    for (auto& batch : out) {
        batches.push_back(batch.ResultBatch);
    }

    auto res = arrow::Table::FromRecordBatches(batches);
    if (!res.ok()) {
        Y_VERIFY_DEBUG(false, "Cannot make table from batches");
        return;
    }

    res = (*res)->CombineChunks();
    if (!res.ok()) {
        Y_VERIFY_DEBUG(false, "Cannot combine chunks");
        return;
    }
    auto batch = NArrow::ToBatch(*res);

    TVector<TPartialReadResult> merged;
    merged.emplace_back(TPartialReadResult{
        .ResultBatch = std::move(batch),
        .LastReadKey = std::move(out.back().LastReadKey)
    });
    out.swap(merged);
}

TVector<TPartialReadResult>
TIndexedReadData::MakeResult(std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>>&& granules,
                             int64_t maxRowsInBatch) const {
    Y_VERIFY(ReadMetadata->IsSorted());
    Y_VERIFY(SortReplaceDescription);

    TVector<TPartialReadResult> out;

    bool isDesc = ReadMetadata->IsDescSorted();

    for (auto& batches : granules) {
        if (batches.empty()) {
            continue;
        }

        {
            std::vector<std::shared_ptr<arrow::RecordBatch>> splitted;
            splitted.reserve(batches.size());
            for (auto& batch : batches) {
                SliceBatch(batch, maxRowsInBatch, splitted);
            }
            batches.swap(splitted);
        }

        if (isDesc) {
            std::vector<std::shared_ptr<arrow::RecordBatch>> reversed;
            reversed.reserve(batches.size());
            for (int i = batches.size() - 1; i >= 0; --i) {
                auto& batch = batches[i];
                auto permutation = NArrow::MakePermutation(batch->num_rows(), true);
                auto res = arrow::compute::Take(batch, permutation);
                Y_VERIFY(res.ok());
                reversed.push_back((*res).record_batch());
            }
            batches.swap(reversed);
        }

        for (auto& batch : batches) {
            Y_VERIFY_DEBUG(NArrow::IsSortedAndUnique(batch, IndexInfo().GetReplaceKey(), isDesc));

            if (batch->num_rows() == 0) {
                Y_VERIFY_DEBUG(false, "Unexpected empty batch");
                continue;
            }

            // Extract the last row's PK
            auto keyBatch = NArrow::ExtractColumns(batch, IndexInfo().GetReplaceKey());
            auto lastKey = keyBatch->Slice(keyBatch->num_rows()-1, 1);

            // Leave only requested columns
            auto resultBatch = NArrow::ExtractColumns(batch, ReadMetadata->ResultSchema);
            out.emplace_back(TPartialReadResult{
                .ResultBatch = std::move(resultBatch),
                .LastReadKey = std::move(lastKey)
            });
        }
    }

    if (ReadMetadata->Program) {
        MergeTooSmallBatches(out);

        for (auto& result : out) {
            auto status = ApplyProgram(result.ResultBatch, *ReadMetadata->Program, NArrow::GetCustomExecContext());
            if (!status.ok()) {
                result.ErrorString = status.message();
            }
        }
    }
    return out;
}

}
