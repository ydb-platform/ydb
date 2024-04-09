#include "read_balancer.h"


namespace NKikimr::NPQ {

//
// TReadingPartitionStatus
//

bool TPersQueueReadBalancer::TReadingPartitionStatus::IsFinished() const {
    return Commited || (ReadingFinished && (StartedReadingFromEndOffset || ScaleAwareSDK));
}

bool TPersQueueReadBalancer::TReadingPartitionStatus::NeedReleaseChildren() const {
     return !(Commited || (ReadingFinished && !ScaleAwareSDK));
}

bool TPersQueueReadBalancer::TReadingPartitionStatus::StartReading() {
    return std::exchange(ReadingFinished, false);
}

bool TPersQueueReadBalancer::TReadingPartitionStatus::SetCommittedState() {
    return !std::exchange(Commited, true);
}

bool TPersQueueReadBalancer::TReadingPartitionStatus::SetFinishedState(bool scaleAwareSDK, bool startedReadingFromEndOffset) {
    ScaleAwareSDK = scaleAwareSDK;
    StartedReadingFromEndOffset = startedReadingFromEndOffset;
    if (!ReadingFinished) {
        ++Iteration;
        ++Cookie;
    }
    return !std::exchange(ReadingFinished, true);
}

bool TPersQueueReadBalancer::TReadingPartitionStatus::Unlock() {
    ReadingFinished = false;
    ++Cookie;
    return NeedReleaseChildren();
}

bool TPersQueueReadBalancer::TReadingPartitionStatus::Reset() {
    ScaleAwareSDK = false;
    ++Cookie;
    return std::exchange(ReadingFinished, false);
};


//
// TSessionInfo
//

void TPersQueueReadBalancer::TSessionInfo::Unlock(bool inactive) {
    --NumActive;
    --NumSuspended;
    if (inactive) {
        -- NumInactive;
    }
}

}
