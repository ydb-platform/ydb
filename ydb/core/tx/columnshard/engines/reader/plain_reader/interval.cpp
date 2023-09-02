#include "interval.h"
#include "scanner.h"

namespace NKikimr::NOlap::NPlainReader {

void TFetchingInterval::ConstructResult() {
    if (Merger && IsSourcesFFReady()) {
        for (auto&& [_, i] : Sources) {
            if (i->GetStart().Compare(Start) == std::partial_ordering::equivalent && !i->IsMergingStarted()) {
                auto rb = i->GetBatch();
                if (rb) {
                    Merger->AddPoolSource({}, rb, i->GetEFData().GetNotAppliedEarlyFilter());
                }
                i->StartMerging();
            }
        }
        Merger->DrainCurrent(RBBuilder, Finish, IncludeFinish);
        Scanner.OnIntervalResult(RBBuilder->Finalize(), GetIntervalIdx());
    }
}

void TFetchingInterval::OnSourceEFReady(const ui32 /*sourceIdx*/) {
    ConstructResult();
}

void TFetchingInterval::OnSourcePKReady(const ui32 /*sourceIdx*/) {
    ConstructResult();
}

void TFetchingInterval::OnSourceFFReady(const ui32 /*sourceIdx*/) {
    ConstructResult();
}

void TFetchingInterval::StartMerge(std::shared_ptr<NIndexedReader::TMergePartialStream> merger) {
    Merger = merger;
//    if (Merger->GetCurrentKeyColumns()) {
//        AFL_VERIFY(Merger->GetCurrentKeyColumns()->Compare(Start) == std::partial_ordering::less)("current", Merger->GetCurrentKeyColumns()->DebugJson())("start", Start.DebugJson());
//    }
    ConstructResult();
}

TFetchingInterval::TFetchingInterval(const NIndexedReader::TSortableBatchPosition& start, const NIndexedReader::TSortableBatchPosition& finish,
    const ui32 intervalIdx, const std::map<ui32, std::shared_ptr<IDataSource>>& sources, TScanHead& scanner,
    std::shared_ptr<NIndexedReader::TRecordBatchBuilder> builder, const bool includeFinish)
    : Scanner(scanner)
    , Start(start)
    , Finish(finish)
    , IncludeFinish(includeFinish)
    , Sources(sources)
    , IntervalIdx(intervalIdx)
    , RBBuilder(builder)
{
    Y_VERIFY(Sources.size());
    for (auto&& [_, i] : Sources) {
        i->RegisterInterval(this);
        Scanner.AddSourceByIdx(i);
        i->NeedEF();
        if (Scanner.GetContext().GetIsInternalRead()) {
            i->NeedPK();
            i->NeedFF();
        }
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
