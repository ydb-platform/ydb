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
    if (!IsExclusiveSource()) {
        for (auto&& [_, i] : Sources) {
            if (i->GetStart().Compare(Start) == std::partial_ordering::equivalent && !i->IsMergingStarted()) {
                auto rb = i->GetBatch();
                if (rb) {
                    Merger->AddPoolSource({}, rb, i->GetFilterStageData().GetNotAppliedEarlyFilter());
                }
                i->StartMerging();
            }
        }
        Merger->DrainCurrentTo(*RBBuilder, Finish, IncludeFinish);
        Scanner.OnIntervalResult(RBBuilder->Finalize(), GetIntervalIdx());
    } else {
        Y_VERIFY(Merger->IsEmpty());
        Sources.begin()->second->StartMerging();
        auto batch = Sources.begin()->second->GetBatch();
        if (batch && batch->num_rows()) {
            if (Scanner.IsReverse()) {
                auto permutation = NArrow::MakePermutation(batch->num_rows(), true);
                batch = NArrow::TStatusValidator::GetValid(arrow::compute::Take(batch, permutation)).record_batch();
            }
            batch = NArrow::ExtractExistedColumns(batch, RBBuilder->GetFields());
            AFL_VERIFY((ui32)batch->num_columns() == RBBuilder->GetFields().size())("batch", batch->num_columns())("builder", RBBuilder->GetFields().size())
                ("batch_columns", JoinSeq(",", batch->schema()->field_names()))("builder_columns", RBBuilder->GetColumnNames());
        }
        Scanner.OnIntervalResult(batch, GetIntervalIdx());
    }
}

void TFetchingInterval::OnSourceFetchStageReady(const ui32 /*sourceIdx*/) {
    ConstructResult();
}

void TFetchingInterval::OnSourceFilterStageReady(const ui32 /*sourceIdx*/) {
    ConstructResult();
}

void TFetchingInterval::StartMerge(std::shared_ptr<NIndexedReader::TMergePartialStream> merger) {
    Y_VERIFY(!Merger);
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
    Y_VERIFY(Sources.size());
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
