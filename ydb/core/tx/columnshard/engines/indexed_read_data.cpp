#include "defs.h"
#include "indexed_read_data.h"
#include "filter.h"
#include "column_engine_logs.h"
#include <ydb/core/tx/columnshard/columnshard__index_scan.h>
#include <ydb/core/tx/columnshard/columnshard__stats_scan.h>
#include <ydb/core/formats/arrow/one_batch_input_stream.h>
#include <ydb/core/formats/arrow/merging_sorted_input_stream.h>
#include <ydb/core/formats/arrow/custom_registry.h>

namespace NKikimr::NOlap {

namespace {

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
        TMap<NArrow::TReplaceKey, std::vector<std::shared_ptr<arrow::RecordBatch>>> points;

        for (auto& batch : batches) {
            auto compositeKey = NArrow::ExtractColumns(batch, indexInfo.GetReplaceKey());
            Y_VERIFY(compositeKey && compositeKey->num_rows() > 0);
            auto keyColumns = std::make_shared<NArrow::TArrayVec>(compositeKey->columns());

            NArrow::TReplaceKey min(keyColumns, 0);
            NArrow::TReplaceKey max(keyColumns, compositeKey->num_rows() - 1);

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
                                                                    const THashSet<const void*>& batchesToDedup) {
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

void TIndexedReadData::AddBlobForFetch(const TBlobRange& range, NIndexedReader::TBatch& batch) {
    Y_VERIFY(IndexedBlobs.emplace(range).second);
    Y_VERIFY(IndexedBlobSubscriber.emplace(range, &batch).second);
    if (batch.GetFetchedInfo().GetFilter()) {
        Counters.PostFilterBytes->Add(range.Size);
        ReadMetadata->ReadStats->DataAdditionalBytes += range.Size;
        FetchBlobsQueue.emplace_front(batch.GetGranule(), range);
    } else {
        Counters.FilterBytes->Add(range.Size);
        ReadMetadata->ReadStats->DataFilterBytes += range.Size;
        FetchBlobsQueue.emplace_back(batch.GetGranule(), range);
    }
}

void TIndexedReadData::InitRead(ui32 inputBatch) {
    auto& indexInfo = ReadMetadata->GetIndexInfo();
    Y_VERIFY(indexInfo.GetSortingKey());
    Y_VERIFY(indexInfo.GetIndexKey() && indexInfo.GetIndexKey()->num_fields());

    SortReplaceDescription = indexInfo.SortReplaceDescription();

    NotIndexed.resize(inputBatch);

    Y_VERIFY(!GranulesContext);
    GranulesContext = std::make_unique<NIndexedReader::TGranulesFillingContext>(ReadMetadata, *this, OnePhaseReadMode);
    ui64 portionsBytes = 0;
    for (auto& portionInfo : ReadMetadata->SelectInfo->GetPortionsOrdered(ReadMetadata->IsDescSorted())) {
        portionsBytes += portionInfo.BlobsBytes();
        Y_VERIFY_S(portionInfo.Records.size(), "ReadMeatadata: " << *ReadMetadata);

        NIndexedReader::TGranule::TPtr granule = GranulesContext->UpsertGranule(portionInfo.Records[0].Granule);
        granule->RegisterBatchForFetching(portionInfo);
    }
    GranulesContext->PrepareForStart();

    Counters.PortionBytes->Add(portionsBytes);
    auto& stats = ReadMetadata->ReadStats;
    stats->IndexGranules = ReadMetadata->SelectInfo->Granules.size();
    stats->IndexPortions = ReadMetadata->SelectInfo->Portions.size();
    stats->IndexBatches = ReadMetadata->NumIndexedBlobs();
    stats->CommittedBatches = ReadMetadata->CommittedBlobs.size();
    stats->SchemaColumns = ReadMetadata->GetSchemaColumnsCount();
    stats->FilterColumns = GranulesContext->GetEarlyFilterColumns().size();
    stats->AdditionalColumns = GranulesContext->GetPostFilterColumns().size();
    stats->PortionsBytes = portionsBytes;
}

void TIndexedReadData::AddIndexed(const TBlobRange& blobRange, const TString& data) {
    Y_VERIFY(GranulesContext);
    NIndexedReader::TBatch* portionBatch = nullptr;
    {
        auto it = IndexedBlobSubscriber.find(blobRange);
        Y_VERIFY_DEBUG(it != IndexedBlobSubscriber.end());
        if (it == IndexedBlobSubscriber.end()) {
            return;
        }
        portionBatch = it->second;
        IndexedBlobSubscriber.erase(it);
    }
    if (!portionBatch->AddIndexedReady(blobRange, data)) {
        return;
    }
    if (portionBatch->IsFetchingReady()) {
        if (auto batch = portionBatch->AssembleTask(TasksProcessor.GetObject(), ReadMetadata)) {
            TasksProcessor.Add(*GranulesContext, batch);
        }
    }
}

std::shared_ptr<arrow::RecordBatch>
TIndexedReadData::MakeNotIndexedBatch(const std::shared_ptr<arrow::RecordBatch>& srcBatch, const TSnapshot& snapshot) const {
    Y_VERIFY(srcBatch);

    // Extract columns (without check), filter, attach snapshot, extract columns with check
    // (do not filter snapshot columns)
    auto dataSchema = ReadMetadata->GetLoadSchema(snapshot);

    auto batch = NArrow::ExtractExistedColumns(srcBatch, dataSchema->GetSchema());
    Y_VERIFY(batch);
    
    auto filter = FilterNotIndexed(batch, *ReadMetadata);
    if (filter.IsTotalDenyFilter()) {
        return nullptr;
    }
    auto preparedBatch = batch;
    preparedBatch = TIndexInfo::AddSpecialColumns(preparedBatch, snapshot);
    auto resultSchema = ReadMetadata->GetLoadSchema();
    preparedBatch = resultSchema->NormalizeBatch(*dataSchema, preparedBatch);
    preparedBatch = NArrow::ExtractColumns(preparedBatch, resultSchema->GetSchema());
    Y_VERIFY(preparedBatch);

    filter.Apply(preparedBatch);
    return preparedBatch;
}

std::vector<TPartialReadResult> TIndexedReadData::GetReadyResults(const int64_t maxRowsInBatch) {
    Y_VERIFY(SortReplaceDescription);
    auto& indexInfo = ReadMetadata->GetIndexInfo();

    if (NotIndexed.size() != ReadyNotIndexed) {
        // Wait till we have all not indexed data so we could replace keys in granules
        return {};
    }

    // First time extract OutNotIndexed data
    if (NotIndexed.size()) {
        auto mergedBatch = MergeNotIndexed(std::move(NotIndexed)); // merged has no dups
        if (mergedBatch) {
            // Init split by granules structures
            Y_VERIFY(ReadMetadata->SelectInfo);
            TColumnEngineForLogs::TMarksGranules marksGranules(*ReadMetadata->SelectInfo);

            // committed data before the first granule would be placed in fake preceding granule
            // committed data after the last granule would be placed into the last granule
            marksGranules.MakePrecedingMark(indexInfo);
            Y_VERIFY(!marksGranules.Empty());

            auto outNotIndexed = marksGranules.SliceIntoGranules(mergedBatch, indexInfo);
            GranulesContext->DrainNotIndexedBatches(&outNotIndexed);
            if (outNotIndexed.size() == 1) {
                auto it = outNotIndexed.begin();
                if (it->first) {
                    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("incorrect_granule_id", it->first);
                    Y_FAIL();
                }
                NotIndexedOutscopeBatch = it->second;
            }
        } else {
            GranulesContext->DrainNotIndexedBatches(nullptr);
        }
        NotIndexed.clear();
        ReadyNotIndexed = 0;
    } else {
        GranulesContext->DrainNotIndexedBatches(nullptr);
    }

    // Extract ready to out granules: ready granules that are not blocked by other (not ready) granules
    Y_VERIFY(GranulesContext);
    auto out = MakeResult(ReadyToOut(), maxRowsInBatch);
    const bool requireResult = GranulesContext->IsFinished(); // not indexed or the last indexed read (even if it's empty)
    if (requireResult && out.empty()) {
        out.push_back(TPartialReadResult{
            .ResultBatch = NArrow::MakeEmptyBatch(ReadMetadata->GetResultSchema())
        });
    }
    return out;
}

/// @return batches that are not blocked by others
std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>> TIndexedReadData::ReadyToOut() {
    Y_VERIFY(SortReplaceDescription);
    Y_VERIFY(GranulesContext);

    auto& indexInfo = ReadMetadata->GetIndexInfo();
    std::vector<NIndexedReader::TGranule::TPtr> ready = GranulesContext->DetachReadyInOrder();
    std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>> out;
    out.reserve(ready.size() + 1);

    // Prepend not indexed data (less then first granule) before granules for ASC sorting
    if (ReadMetadata->IsAscSorted() && NotIndexedOutscopeBatch) {
        out.push_back({});
        out.back().push_back(NotIndexedOutscopeBatch);
        NotIndexedOutscopeBatch = nullptr;
    }

    for (auto&& granule : ready) {
        std::vector<std::shared_ptr<arrow::RecordBatch>> inGranule = granule->GetReadyBatches();

        if (inGranule.empty()) {
            continue;
        }

        if (granule->IsDuplicationsAvailable()) {
            for (auto& batch : inGranule) {
                Y_VERIFY(batch->num_rows());
                Y_VERIFY_DEBUG(NArrow::IsSorted(batch, SortReplaceDescription->ReplaceKey));
            }
            auto deduped = SpecialMergeSorted(inGranule, indexInfo, SortReplaceDescription, granule->GetBatchesToDedup());
            out.emplace_back(std::move(deduped));
        } else {
            out.emplace_back(std::move(inGranule));
        }
    }

    // Append not indexed data (less then first granule) after granules for DESC sorting
    if (GranulesContext->GetSortingPolicy()->ReadyForAddNotIndexedToEnd() && NotIndexedOutscopeBatch) {
        out.push_back({});
        out.back().push_back(NotIndexedOutscopeBatch);
        NotIndexedOutscopeBatch = nullptr;
    }

    return out;
}

std::shared_ptr<arrow::RecordBatch>
TIndexedReadData::MergeNotIndexed(std::vector<std::shared_ptr<arrow::RecordBatch>>&& batches) const {
    Y_VERIFY(ReadMetadata->IsSorted());
    auto& indexInfo = ReadMetadata->GetIndexInfo();
    Y_VERIFY(indexInfo.GetSortingKey());

    {
        const auto pred = [](const std::shared_ptr<arrow::RecordBatch>& b) {
            return !b || !b->num_rows();
        };
        batches.erase(std::remove_if(batches.begin(), batches.end(), pred), batches.end());
    }

    if (batches.empty()) {
        return {};
    }

    // We could merge data here only if backpressure limits committed data size. KIKIMR-12520
    auto merged = NArrow::CombineSortedBatches(batches, indexInfo.SortReplaceDescription());
    Y_VERIFY(merged);
    Y_VERIFY_DEBUG(NArrow::IsSortedAndUnique(merged, indexInfo.GetReplaceKey()));
    return merged;
}

// TODO: better implementation
static void MergeTooSmallBatches(std::vector<TPartialReadResult>& out) {
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

    std::vector<TPartialReadResult> merged;
    merged.emplace_back(TPartialReadResult{
        .ResultBatch = std::move(batch),
        .LastReadKey = std::move(out.back().LastReadKey)
    });
    out.swap(merged);
}

std::vector<TPartialReadResult>
TIndexedReadData::MakeResult(std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>>&& granules,
                             int64_t maxRowsInBatch) const {
    Y_VERIFY(ReadMetadata->IsSorted());
    Y_VERIFY(SortReplaceDescription);
    auto& indexInfo = ReadMetadata->GetIndexInfo();

    std::vector<TPartialReadResult> out;

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
            Y_VERIFY_DEBUG(NArrow::IsSortedAndUnique(batch, indexInfo.GetReplaceKey(), isDesc));

            if (batch->num_rows() == 0) {
                Y_VERIFY_DEBUG(false, "Unexpected empty batch");
                continue;
            }

            // Extract the last row's PK
            auto keyBatch = NArrow::ExtractColumns(batch, indexInfo.GetReplaceKey());
            auto lastKey = keyBatch->Slice(keyBatch->num_rows()-1, 1);

            // Leave only requested columns
            auto resultBatch = NArrow::ExtractColumns(batch, ReadMetadata->GetResultSchema());
            out.emplace_back(TPartialReadResult{
                .ResultBatch = std::move(resultBatch),
                .LastReadKey = std::move(lastKey)
            });
        }
    }
    
    if (ReadMetadata->GetProgram().HasProgram()) {
        MergeTooSmallBatches(out);
        for (auto& result : out) {
            auto status = ReadMetadata->GetProgram().ApplyProgram(result.ResultBatch);
            if (!status.ok()) {
                result.ErrorString = status.message();
            }
        }
    }
    return out;
}

TIndexedReadData::TIndexedReadData(NOlap::TReadMetadata::TConstPtr readMetadata,
    const bool internalRead, const NColumnShard::TConcreteScanCounters& counters, NColumnShard::TDataTasksProcessorContainer tasksProcessor)
    : Counters(counters)
    , TasksProcessor(tasksProcessor)
    , ReadMetadata(readMetadata)
    , OnePhaseReadMode(internalRead)
{
    Y_VERIFY(ReadMetadata->SelectInfo);
}

bool TIndexedReadData::IsFinished() const {
    Y_VERIFY(GranulesContext);
    return NotIndexed.empty() && FetchBlobsQueue.empty() && GranulesContext->IsFinished();
}

void TIndexedReadData::Abort() {
    Y_VERIFY(GranulesContext);
    FetchBlobsQueue.Stop();
    GranulesContext->Abort();
}

}
