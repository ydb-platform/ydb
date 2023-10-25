#include "interval.h"
#include "scanner.h"
#include "plain_read_data.h"
#include <ydb/core/tx/conveyor/usage/service.h>

namespace NKikimr::NOlap::NPlainReader {

bool TFetchingInterval::IsExclusiveSource() const {
    return IncludeStart && Sources.size() == 1 && IncludeFinish;
}

class TMergeTask: public NColumnShard::IDataTasksProcessor::ITask, public TMergingContext {
private:
    std::shared_ptr<NIndexedReader::TMergePartialStream> Merger;
    std::shared_ptr<arrow::RecordBatch> ResultBatch;
    NColumnShard::TConcreteScanCounters Counters;
    const NColumnShard::TCounterGuard Guard;
    const bool IsExclusiveSource = false;
    const ui32 OriginalSourcesCount;
protected:
    virtual bool DoApply(NOlap::IDataReader& indexedDataRead) const override {
        auto& reader = static_cast<TPlainReadData&>(indexedDataRead);
        if (Merger->GetSourcesCount() == 1 && ResultBatch) {
            auto batch = NArrow::ExtractColumnsValidate(ResultBatch, reader.GetScanner().GetResultFieldNames());
            AFL_VERIFY(batch);
            reader.MutableScanner().OnIntervalResult(batch, IntervalIdx);
        } else {
            reader.MutableScanner().OnIntervalResult(ResultBatch, IntervalIdx);
        }
        return true;
    }
    virtual bool DoExecute() override {
        Merger->SkipToLowerBound(Start, IncludeStart);
        if (Merger->GetSourcesCount() == 1) {
            ResultBatch = Merger->SingleSourceDrain(Finish, IncludeFinish);
            if (ResultBatch) {
                if (IsExclusiveSource) {
                    Counters.OnNoScanInterval(ResultBatch->num_rows());
                } else {
                    Counters.OnLogScanInterval(ResultBatch->num_rows());
                }
            }
            if (IncludeFinish && OriginalSourcesCount == 1) {
                Y_ABORT_UNLESS(Merger->IsEmpty());
            }
        } else {
            Merger->DrainCurrentTo(*RBBuilder, Finish, IncludeFinish);
            Counters.OnLinearScanInterval(RBBuilder->GetRecordsCount());
            ResultBatch = RBBuilder->Finalize();
        }
        return true;
    }
public:
    virtual TString GetTaskClassIdentifier() const override {
        return "CS::MERGE_RESULT";
    }

    TMergeTask(const std::shared_ptr<NIndexedReader::TMergePartialStream>& merger,
        const TMergingContext& context, const NColumnShard::TConcreteScanCounters& counters, const bool isExclusiveSource, const ui32 sourcesCount)
        : TMergingContext(context)
        , Merger(merger)
        , Counters(counters)
        , Guard(Counters.GetMergeTasksGuard())
        , IsExclusiveSource(isExclusiveSource)
        , OriginalSourcesCount(sourcesCount)
    {
    }
};

void TFetchingInterval::ConstructResult() {
    if (!IsSourcesReady()) {
        return;
    }
    AFL_VERIFY(!ResultConstructionInProgress);
    ResultConstructionInProgress = true;
    auto merger = Scanner.BuildMerger();
    for (auto&& [_, i] : Sources) {
        if (auto rb = i->GetBatch()) {
            merger->AddSource(rb, i->GetFilterStageData().GetNotAppliedEarlyFilter());
        }
    }
    AFL_VERIFY(merger->GetSourcesCount() <= Sources.size());
    if (merger->GetSourcesCount() == 0) {
        Scanner.OnIntervalResult(nullptr, IntervalIdx);
    } else {
        auto task = std::make_shared<TMergeTask>(merger, *this, Scanner.GetContext().GetCounters(), IsExclusiveSource(), Sources.size());
        task->SetPriority(NConveyor::ITask::EPriority::High);
        NConveyor::TScanServiceOperator::SendTaskToExecute(task);
    }
}

void TFetchingInterval::OnSourceFetchStageReady(const ui32 /*sourceIdx*/) {
    ConstructResult();
}

void TFetchingInterval::OnSourceFilterStageReady(const ui32 /*sourceIdx*/) {
    ConstructResult();
}

TFetchingInterval::TFetchingInterval(const NIndexedReader::TSortableBatchPosition& start, const NIndexedReader::TSortableBatchPosition& finish,
    const ui32 intervalIdx, const std::map<ui32, std::shared_ptr<IDataSource>>& sources, TScanHead& scanner,
    std::shared_ptr<NIndexedReader::TRecordBatchBuilder> builder, const bool includeFinish, const bool includeStart)
    : TBase(start, finish, intervalIdx, builder, includeFinish, includeStart)
    , Scanner(scanner)
    , Sources(sources)
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
