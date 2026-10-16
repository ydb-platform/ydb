#include "classifier_metadata_tracker.h"

#include <utility>


namespace NKikimr::NWorkloadManager::NPrivate {

TClassifierMetadataTracker::TClassifierMetadataTracker(TDuration requestTimeout)
    : RequestTimeout_(requestTimeout)
{}

void TClassifierMetadataTracker::Init(bool metadataServiceEnabled, TInstant now) {
    SetState(metadataServiceEnabled ? EMetadataState::Pending : EMetadataState::Ready, now);
}

EMetadataState TClassifierMetadataTracker::GetState() const {
    return State_;
}

bool TClassifierMetadataTracker::IsInFlight() const {
    return PoolsEnabled_ && State_ == EMetadataState::Pending;
}

bool TClassifierMetadataTracker::OnPoolsEnabled(bool enabled, TInstant now) {
    const bool wasEnabled = std::exchange(PoolsEnabled_, enabled);
    if (wasEnabled || !enabled || State_ != EMetadataState::Pending) {
        return false;
    }
    SetState(EMetadataState::Pending, now);
    return true;
}

bool TClassifierMetadataTracker::OnReady(TInstant now) {
    if (State_ == EMetadataState::Ready) {
        return false;
    }
    SetState(EMetadataState::Ready, now);
    return true;
}

bool TClassifierMetadataTracker::TimeOutPending(TInstant now) {
    if (!IsInFlight() || now - StateAt_ <= RequestTimeout_) {
        return false;
    }
    SetState(EMetadataState::TimedOut, now);
    return true;
}

bool TClassifierMetadataTracker::NeedsRequery(TInstant now) {
    if (!PoolsEnabled_ || State_ != EMetadataState::TimedOut || now - StateAt_ <= RequestTimeout_) {
        return false;
    }
    StateAt_ = now;
    return true;
}

void TClassifierMetadataTracker::SetState(EMetadataState state, TInstant now) {
    State_ = state;
    StateAt_ = now;
}

}
