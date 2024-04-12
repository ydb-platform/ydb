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

bool TPersQueueReadBalancer::TReadingPartitionStatus::StopReading() {
    ReadingFinished = false;
    ++Cookie;
    return NeedReleaseChildren();
}

bool TPersQueueReadBalancer::TReadingPartitionStatus::SetCommittedState() {
    Iteration = 0;
    return !std::exchange(Commited, true);
}

bool TPersQueueReadBalancer::TReadingPartitionStatus::SetFinishedState(bool scaleAwareSDK, bool startedReadingFromEndOffset) {
    bool previousStatus = IsFinished();

    ScaleAwareSDK = scaleAwareSDK;
    StartedReadingFromEndOffset = startedReadingFromEndOffset;
    ReadingFinished = true;
    ++Cookie;

    bool currentStatus = IsFinished();
    if (currentStatus) {
        Iteration = 0;
    } else {
        ++Iteration;
    }
    return currentStatus && !previousStatus;
}

bool TPersQueueReadBalancer::TReadingPartitionStatus::Reset() {
    bool result = IsFinished();

    ScaleAwareSDK = false;
    ReadingFinished = false;
    Commited = false;
    ++Cookie;

    return result;
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
