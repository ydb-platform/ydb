#include "progress_merger.h"


namespace NYql::NProgressMerger {

//////////////////////////////////////////////////////////////////////////////
// TNodeProgressBase
//////////////////////////////////////////////////////////////////////////////

TNodeProgressBase::TNodeProgressBase(const TOperationProgress& p)
    : Progress_(p)
    , StartedAt_(TInstant::Now())
    , FinishedAt_(p.State == EState::Finished ? StartedAt_ : TInstant::Max())
    , Dirty_(true)
{
    if (!p.Stage.first.empty()) {
        Stages_.emplace_back(p.Stage);
    }
}

TNodeProgressBase::TNodeProgressBase(
    const TOperationProgress& p,
    TInstant startedAt,
    TInstant finishedAt,
    const TVector<TOperationProgress::TStage>& stages)
    : Progress_(p)
    , StartedAt_(startedAt)
    , FinishedAt_(finishedAt)
    , Stages_(stages)
    , Dirty_(true)
{}


bool TNodeProgressBase::MergeWith(const TOperationProgress& p) {
    bool dirty = false;

    // (1) remote id
    if (!p.RemoteId.empty() && p.RemoteId != Progress_.RemoteId) {
        Progress_.RemoteId = p.RemoteId;
        dirty = true;
    }

    // (2) state
    if (p.State != Progress_.State) {
        Progress_.State = p.State;
        dirty = true;
    }

    // (3) counters
    if (p.Counters && (!Progress_.Counters || *p.Counters != *Progress_.Counters)) {
        Progress_.Counters = p.Counters;
        dirty = true;
    }

    // (4) finished time
    if (Progress_.State == EState::Finished) {
        FinishedAt_ = TInstant::Now();
        dirty = true;
    }

    // (5) stage
    if (!p.Stage.first.empty() &&  Progress_.Stage != p.Stage) {
        Progress_.Stage = p.Stage;
        Stages_.push_back(p.Stage);
        dirty = true;
    }

    // (6) remote data
    if (!p.RemoteData.empty() && p.RemoteData != Progress_.RemoteData) {
        Progress_.RemoteData = p.RemoteData;
        dirty = true;
    }
    return Dirty_ = dirty;
}

void TNodeProgressBase::Abort() {
    Progress_.State = EState::Aborted;
    FinishedAt_ = TInstant::Now();
    Dirty_ = true;
}

bool TNodeProgressBase::IsUnfinished() const {
    return Progress_.State == EState::Started ||
            Progress_.State == EState::InProgress;
}

bool TNodeProgressBase::IsDirty() const {
    return Dirty_;
}

void TNodeProgressBase::SetDirty(bool dirty) {
    Dirty_ = dirty;
}

} // namespace NYql::NProgressMerger
