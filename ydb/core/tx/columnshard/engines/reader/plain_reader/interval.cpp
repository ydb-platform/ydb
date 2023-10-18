#include "interval.h"
#include "scanner.h"

namespace NKikimr::NOlap::NPlainReader {

bool TFetchingInterval::IsExclusiveSource() const {
    return IncludeStart && Sources.size() == 1 && IncludeFinish;
}

void TFetchingInterval::ConstructResult() {
    if (!Merger || !IsSourcesReady()) {
        return;
    }
    for (auto&& [_, i] : Sources) {
        if (i->GetStart().Compare(Start) == std::partial_ordering::equivalent && !i->IsMergingStarted()) {
            if (auto rb = i->GetBatch()) {
                Merger->AddSource(rb, i->GetFilterStageData().GetNotAppliedEarlyFilter());
            }
            i->StartMerging();
        }
    }
    std::shared_ptr<arrow::RecordBatch> simpleBatch;
    AFL_VERIFY(Merger->GetSourcesCount() <= Sources.size());
    if (Sources.size() == 1) {
        simpleBatch = Merger->SingleSourceDrain(Finish, IncludeFinish);
        if (simpleBatch) {
            if (IsExclusiveSource()) {
                Scanner.GetContext().GetCounters().OnNoScanInterval(simpleBatch->num_rows());
            } else {
                Scanner.GetContext().GetCounters().OnLogScanInterval(simpleBatch->num_rows());
            }
            simpleBatch = NArrow::ExtractColumnsValidate(simpleBatch, Scanner.GetResultFieldNames());
            AFL_VERIFY(simpleBatch);
        }
        if (IncludeFinish) {
            Y_ABORT_UNLESS(Merger->IsEmpty());
        }
    } else {
        Merger->DrainCurrentTo(*RBBuilder, Finish, IncludeFinish);
        Scanner.GetContext().GetCounters().OnLinearScanInterval(RBBuilder->GetRecordsCount());
        simpleBatch = RBBuilder->Finalize();
    }
    Scanner.OnIntervalResult(simpleBatch, GetIntervalIdx());
}

void TFetchingInterval::OnSourceFetchStageReady(const ui32 /*sourceIdx*/) {
    ConstructResult();
}

void TFetchingInterval::OnSourceFilterStageReady(const ui32 /*sourceIdx*/) {
    ConstructResult();
}

void TFetchingInterval::StartMerge(std::shared_ptr<NIndexedReader::TMergePartialStream> merger) {
    Y_ABORT_UNLESS(!Merger);
    Merger = merger;
    ConstructResult();
}

TFetchingInterval::TFetchingInterval(const NIndexedReader::TSortableBatchPosition& start, const NIndexedReader::TSortableBatchPosition& finish,
    const ui32 intervalIdx, const std::map<ui32, std::shared_ptr<IDataSource>>& sources, TScanHead& scanner,
    std::shared_ptr<NIndexedReader::TRecordBatchBuilder> builder, const bool includeFinish, const bool includeStart)
    : Scanner(scanner)
    , Start(start)
    , Finish(finish)
    , IncludeFinish(includeFinish)
    , IncludeStart(includeStart)
    , Sources(sources)
    , IntervalIdx(intervalIdx)
    , RBBuilder(builder)
{
    Y_ABORT_UNLESS(Sources.size());
    for (auto&& [_, i] : Sources) {
        i->RegisterInterval(this);
        Scanner.AddSourceByIdx(i);
        i->InitFetchingPlan(Scanner.GetColumnsFetchingPlan(IsExclusiveSource()));
    }
}

TFetchingInterval::~TFetchingInterval() {
    for (auto&& [_, s] : Sources) {
        if (s->OnIntervalFinished(IntervalIdx)) {
            Scanner.RemoveSourceByIdx(s);
        }
    }
}

}
