#include "splitter.h"

namespace NKikimr::NOlap {

TSplitLimiter::TSplitLimiter(const TGranuleMeta* granuleMeta, const NColumnShard::TIndexationCounters& counters, ISnapshotSchema::TPtr schema, const std::shared_ptr<arrow::RecordBatch> batch): Counters(counters)
, Batch(batch)
, Schema(schema) {
    if (granuleMeta && granuleMeta->GetAdditiveSummary().GetOther().GetRecordsCount()) {
        Y_VERIFY(granuleMeta->GetHardSummary().GetColumnIdsSortedBySizeDescending().size());
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

bool TSplitLimiter::Next(std::vector<TString>& portionBlobs, std::shared_ptr<arrow::RecordBatch>& batch, const TSaverContext& saverContext) {
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

}
