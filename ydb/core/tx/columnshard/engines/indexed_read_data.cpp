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
    int64_t offset = 0;
    while (offset < batch->num_rows()) {
        int64_t rows = std::min<int64_t>(maxRowsInBatch, batch->num_rows() - offset);
        result.emplace_back(batch->Slice(offset, rows));
        offset += rows;
    }
};

std::vector<std::shared_ptr<arrow::RecordBatch>> SpecialMergeSorted(const std::vector<std::shared_ptr<arrow::RecordBatch>>& src,
                                                                    const TIndexInfo& indexInfo,
                                                                    const std::shared_ptr<NArrow::TSortDescription>& description,
                                                                    const int64_t maxRowsInBatch) {
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    batches.reserve(src.size());
    ui64 size = 0;
    for (auto& batch : src) {
        if (!batch->num_rows()) {
            continue;
        }
        Y_VERIFY_DEBUG(NArrow::IsSorted(batch, description->ReplaceKey));

        size += batch->num_rows();
        batches.push_back(batch);
    }
    if (batches.empty()) {
        return {};
    }

#if 1 // Optimization [remove portion's dups]
    if (batches.size() == 1) {
        if (NArrow::IsSortedAndUnique(batches[0], description->ReplaceKey)) {
            std::vector<std::shared_ptr<arrow::RecordBatch>> out;
            SliceBatch(batches[0], maxRowsInBatch, out);
            return out;
        } else {
            return NArrow::MergeSortedBatches(batches, description, size);
        }
    }
#endif

#if 1 // Optimization [special merge], requires [remove portion's dups]
    TVector<TVector<std::shared_ptr<arrow::RecordBatch>>> rangesSlices; // rangesSlices[rangeNo][sliceNo]
    rangesSlices.reserve(batches.size());
    {
        TMap<TMark, TVector<std::shared_ptr<arrow::RecordBatch>>> points;

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

    // Merge slices in ranges
    std::vector<std::shared_ptr<arrow::RecordBatch>> out;
    out.reserve(rangesSlices.size());
    for (auto& slices : rangesSlices) {
        if (slices.empty()) {
            continue;
        }

        // The core of optimization: do not merge slice if it's alone in its key range
        if (slices.size() == 1) {
            if (NArrow::IsSortedAndUnique(slices[0], description->ReplaceKey)) {
                // Split big batch into smaller batches if needed
                SliceBatch(slices[0], maxRowsInBatch, out);
                continue;
            }
        }

        auto merged = NArrow::MergeSortedBatches(slices, description, maxRowsInBatch);
        Y_VERIFY(merged.size() >= 1);
        out.insert(out.end(), merged.begin(), merged.end());
    }

    return out;
#else
    Y_UNUSED(indexInfo);
    return NArrow::MergeSortedBatches(batches, description, size);
#endif
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

        // If no PK dups in portions we could use optimized version of merge
        if (portionInfo.CanHaveDups()) {
            PortionsWithSelfDups.emplace(granule);
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
        Indexed[batchNo] = AssembleIndexedBatch(batchNo);
        UpdateGranuleWaits(batchNo);
    }
}

std::shared_ptr<arrow::RecordBatch> TIndexedReadData::AssembleIndexedBatch(ui32 batchNo) {
    auto& portionInfo = Portion(batchNo);

    auto portion = portionInfo.Assemble(ReadMetadata->IndexInfo, ReadMetadata->LoadSchema, Data);
    Y_VERIFY(portion);
    auto batch = NOlap::FilterPortion(portion, *ReadMetadata);
    Y_VERIFY(batch);

    for (auto& rec : portionInfo.Records) {
        auto& blobRange = rec.BlobRange;
        Data.erase(blobRange);
    }

    return batch;
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
TIndexedReadData::MakeNotIndexedBatch(const std::shared_ptr<arrow::RecordBatch>& srcBatch,
                                      ui64 planStep, ui64 txId) const {
    Y_VERIFY(srcBatch);

    // Extract columns (without check), filter, attach snapshot, extract columns with check
    // (do not filter snapshot columns)

    auto batch = NArrow::ExtractExistedColumns(srcBatch, ReadMetadata->LoadSchema);
    Y_VERIFY(batch);

    { // Apply predicate
        // TODO: Extract this info function
        std::vector<bool> less;
        if (ReadMetadata->LessPredicate) {
            Y_VERIFY(NArrow::HasAllColumns(batch, ReadMetadata->LessPredicate->Batch->schema()));

            auto cmpType = ReadMetadata->LessPredicate->Inclusive ?
                NArrow::ECompareType::LESS_OR_EQUAL : NArrow::ECompareType::LESS;
            less = NArrow::MakePredicateFilter(batch, ReadMetadata->LessPredicate->Batch, cmpType);
        }

        std::vector<bool> greater;
        if (ReadMetadata->GreaterPredicate) {
            Y_VERIFY(NArrow::HasAllColumns(batch, ReadMetadata->GreaterPredicate->Batch->schema()));

            auto cmpType = ReadMetadata->GreaterPredicate->Inclusive ?
                NArrow::ECompareType::GREATER_OR_EQUAL : NArrow::ECompareType::GREATER;
            greater = NArrow::MakePredicateFilter(batch, ReadMetadata->GreaterPredicate->Batch, cmpType);
        }

        std::vector<bool> bits = NArrow::CombineFilters(std::move(less), std::move(greater));
        if (bits.size()) {
            auto res = arrow::compute::Filter(batch, NArrow::MakeFilter(bits));
            Y_VERIFY_S(res.ok(), res.status().message());
            Y_VERIFY((*res).kind() == arrow::Datum::RECORD_BATCH);
            batch = (*res).record_batch();
        }
    }

    batch = TIndexInfo::AddSpecialColumns(batch, planStep, txId);
    Y_VERIFY(batch);

    batch = NArrow::ExtractColumns(batch, ReadMetadata->LoadSchema);
    Y_VERIFY(batch);
    return batch;
}

TVector<TPartialReadResult> TIndexedReadData::GetReadyResults(const int64_t maxRowsInBatch) {
    if (NotIndexed.size() != ReadyNotIndexed) {
        // Wait till we have all not indexed data so we could replace keys in granules
        return {};
    }

    // First time extract OutNotIndexed data
    if (NotIndexed.size()) {
        /// @note not indexed data could contain data out of indexed granules
        OutNotIndexed = SplitByGranules(std::move(NotIndexed));
        NotIndexed.clear();
        ReadyNotIndexed = 0;
    }

    // Extact ready granules (they are ready themselves but probably not ready to go out)
    TVector<ui32> ready;
    for (auto& [batchNo, batch] : Indexed) {
        ui64 granule = BatchGranule(batchNo);
        if (ReadyGranules.count(granule)) {
            Y_VERIFY(batch);
            ui64 portion = BatchPortion[batchNo];
#if 1 // Optimization [remove portion's dups]
            // There could be PK self dups if portion is result of insert (same PK, different snapshot). Remove them.
            if (batch->num_rows() && PortionsWithSelfDups.count(portion)) {
                auto merged = NArrow::MergeSortedBatches({batch}, SortReplaceDescription, batch->num_rows());
                Y_VERIFY(merged.size() == 1);
                batch = merged[0];
            }
#endif
            ReadyGranules[granule].emplace(portion, batch);
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
        out.push_back({NArrow::MakeEmptyBatch(ReadMetadata->ResultSchema), nullptr});
    }
    return out;
}

template <typename TCont>
static TVector<ui64> GetReadyInOrder(const TCont& ready, TDeque<ui64>& order) {
    TVector<ui64> out;
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
TVector<std::vector<std::shared_ptr<arrow::RecordBatch>>> TIndexedReadData::ReadyToOut() {
    TVector<std::vector<std::shared_ptr<arrow::RecordBatch>>> out;
    out.reserve(ReadyGranules.size() + 1);

    // Prepend not indexed data (less then first granule) before granules for ASC sorting
    if (ReadMetadata->IsAscSorted() && OutNotIndexed.count(0)) {
        out.push_back({});
        out.back().push_back(OutNotIndexed[0]);
        OutNotIndexed.erase(0);
    }

    TVector<ui64> ready = GetReadyInOrder(ReadyGranules, GranulesOutOrder);
    for (ui64 granule : ready) {
        auto& map = ReadyGranules[granule];
        std::vector<std::shared_ptr<arrow::RecordBatch>> inGranule;

        // Add indexed granule data
        for (auto& [portion, batch] : map) {
            // batch could be empty cause of prefiltration
            if (batch->num_rows()) {
                inGranule.push_back(batch);
            }
        }

        // Append not indexed data to granules
        if (OutNotIndexed.count(granule)) {
            auto batch = OutNotIndexed[granule];
            if (batch->num_rows()) { // TODO: check why it could be empty
                inGranule.push_back(batch);
            }
            OutNotIndexed.erase(granule);
        }

        if (inGranule.empty()) {
            inGranule.push_back(NArrow::MakeEmptyBatch(ReadMetadata->ResultSchema));
        }
        out.push_back(std::move(inGranule));
        ReadyGranules.erase(granule);
    }

    // Append not indexed data (less then first granule) after granules for DESC sorting
    if (ReadMetadata->IsDescSorted() && GranulesOutOrder.empty() && OutNotIndexed.count(0)) {
        out.push_back({});
        out.back().push_back(OutNotIndexed[0]);
        OutNotIndexed.erase(0);
    }

    return out;
}

THashMap<ui64, std::shared_ptr<arrow::RecordBatch>>
TIndexedReadData::SplitByGranules(std::vector<std::shared_ptr<arrow::RecordBatch>>&& batches) const {
    Y_VERIFY(ReadMetadata->IsSorted());
    Y_VERIFY(IndexInfo().GetSortingKey());

    { // remove empty batches
        size_t dst = 0;
        for (size_t src = 0; src < batches.size(); ++src) {
            if (batches[src]->num_rows()) {
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

    Y_VERIFY(!TsGranules.empty());
    return SliceIntoGranules(merged, TsGranules, indexInfo);
}

TVector<TPartialReadResult>
TIndexedReadData::MakeResult(TVector<std::vector<std::shared_ptr<arrow::RecordBatch>>>&& granules, const int64_t maxRowsInBatch) const {
    /// @warning The replace logic is correct only in assumption that predicate is applyed over a part of ReplaceKey.
    /// It's not OK to apply predicate before replacing key duplicates otherwise.
    /// Assumption: dup(A, B) <=> PK(A) = PK(B) => Predicate(A) = Predicate(B) => all or no dups for PK(A) here

    Y_VERIFY(ReadMetadata->IsSorted());
    Y_VERIFY(SortReplaceDescription);

    TVector<TPartialReadResult> out;

    bool isDesc = ReadMetadata->IsDescSorted();

    for (auto& vec : granules) {
        auto batches = SpecialMergeSorted(vec, IndexInfo(), SortReplaceDescription, maxRowsInBatch);
        if (batches.empty()) {
            continue;
        }

        if (isDesc) {
            TVector<std::shared_ptr<arrow::RecordBatch>> reversed;
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
            out.emplace_back(TPartialReadResult{std::move(resultBatch), std::move(lastKey)});
        }
    }

    if (ReadMetadata->Program) {
        for (auto& batch : out) {
            auto status = ApplyProgram(batch.ResultBatch, ReadMetadata->Program->Steps, NArrow::GetCustomExecContext());
            Y_VERIFY_S(status.ok(), status.message());
        }
    }
    return out;
}

}
