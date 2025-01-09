#include "erasure_checkers.h"

#include <ydb/core/protos/counters_cms.pb.h>
#include <ydb/core/tablet/tablet_counters.h>

namespace NKikimr::NCms {

bool TErasureCounterBase::IsDown(const TVDiskInfo &vdisk, TClusterInfoPtr info, TDuration &retryTime, TErrorInfo &error) {
    const auto &node = info->Node(vdisk.NodeId);
    const auto &pdisk = info->PDisk(vdisk.PDiskId);
    const auto defaultDeadline = TActivationContext::Now() + retryTime;

    // Check we received info for PDisk.
    if (!pdisk.NodeId) {
        error.Code = TStatus::DISALLOW_TEMP;
        error.Reason = TStringBuilder() << vdisk.PrettyItemName() << " has missing info for " << pdisk.PrettyItemName();
        Down.emplace(vdisk.VDiskId, error.Reason);
        return false;
    }

    return (node.NodeId != VDisk.NodeId && node.IsDown(error, defaultDeadline))
        || (pdisk.PDiskId != VDisk.PDiskId && pdisk.IsDown(error, defaultDeadline))
        || vdisk.IsDown(error, defaultDeadline);
}

bool TErasureCounterBase::IsLocked(const TVDiskInfo &vdisk, TClusterInfoPtr info, TDuration &retryTime,
        TDuration &duration, TErrorInfo &error)
{
    const auto &node = info->Node(vdisk.NodeId);
    const auto &pdisk = info->PDisk(vdisk.PDiskId);

    // Check we received info for VDisk.
    if (!vdisk.NodeId || !vdisk.PDiskId) {
        error.Code = TStatus::DISALLOW_TEMP;
        error.Reason = TStringBuilder() << vdisk.PrettyItemName() << " has missing info";
        Down.emplace(vdisk.VDiskId, error.Reason);
        return false;
    }

    return node.IsLocked(error, retryTime, TActivationContext::Now(), duration)
        || pdisk.IsLocked(error, retryTime, TActivationContext::Now(), duration)
        || vdisk.IsLocked(error, retryTime, TActivationContext::Now(), duration);
}

bool TErasureCounterBase::GroupAlreadyHasLockedDisks() const {
    return HasAlreadyLockedDisks;
}

bool TErasureCounterBase::GroupHasMoreThanOneDiskPerNode() const {
    return HasMoreThanOneDiskPerNode;
}

static TString DumpVDisksInfo(const THashMap<TVDiskID, TString>& vdisks, TClusterInfoPtr info) {
    if (vdisks.empty()) {
        return "<empty>";
    }

    TStringBuilder dump;

    bool comma = false;
    for (const auto& [vdisk, reason] : vdisks) {
        if (comma) {
            dump << ", ";
        }
        comma = true;

        if (reason) {
            dump << reason;
        } else {
            dump << info->VDisk(vdisk).PrettyItemName();
        }
    }

    return dump;
}

bool TErasureCounterBase::CheckForMaxAvailability(TClusterInfoPtr info, TErrorInfo &error, TInstant &defaultDeadline, bool allowPartial) const {
    if (Locked.size() + Down.size() > 1) {
        if (HasAlreadyLockedDisks && !allowPartial) {
            error.Code = TStatus::DISALLOW;
            error.Reason = "The request is incorrect: too many disks from the one group. "
                           "Fix the request or set PartialPermissionAllowed to true";
            return false;
        }

        error.Code = TStatus::DISALLOW_TEMP;
        error.Reason = TReason(
            TStringBuilder() << "Issue in affected group with id '" << GroupId << "'"
            << ": too many unavailable vdisks"
            << ". Locked: " << DumpVDisksInfo(Locked, info)
            << ". Down: " << DumpVDisksInfo(Down, info),
            TReason::EType::TooManyUnavailableVDisks
        );
        error.Deadline = defaultDeadline;
        return false;
    }

    return true;
}

bool TErasureCounterBase::CountVDisk(const TVDiskInfo &vdisk, TClusterInfoPtr info, TDuration retryTime,
        TDuration duration, TErrorInfo &error)
{
    Y_DEBUG_ABORT_UNLESS(vdisk.VDiskId != VDisk.VDiskId);

    // Check locks.
    TErrorInfo err;
    if (IsLocked(vdisk, info, retryTime, duration, err)) {
        Locked.emplace(vdisk.VDiskId, err.Reason);
        error.Code = err.Code;
        error.Reason = TStringBuilder() << "Issue in affected group with id '" << GroupId << "'"
            << ": " << err.Reason;
        error.Deadline = Max(error.Deadline, err.Deadline);
        return true;
    }

    // Check if disk is down.
    if (IsDown(vdisk, info, retryTime, err)) {
        Down.emplace(vdisk.VDiskId, err.Reason);
        error.Code = err.Code;
        error.Reason = TStringBuilder() << "Issue in affected group with id '" << GroupId << "'"
            << ": " << err.Reason;
        error.Deadline = Max(error.Deadline, err.Deadline);
        return true;
    }

    return false;
}

void TErasureCounterBase::CountGroupState(TClusterInfoPtr info, TDuration retryTime, TDuration duration, TErrorInfo &error) {
    const auto& group = info->BSGroup(GroupId);

    TSet<ui32> groupNodes;
    for (const auto &vdId : group.VDisks) {
        const auto &vd = info->VDisk(vdId);
        if (vd.VDiskId != VDisk.VDiskId)
            CountVDisk(vd, info, retryTime, duration, error);
        groupNodes.insert(vd.NodeId);
    }

    HasMoreThanOneDiskPerNode = group.VDisks.size() > groupNodes.size();

    if (Locked && error.Code == TStatus::DISALLOW) {
        HasAlreadyLockedDisks = true;
    }

    Locked.emplace(VDisk.VDiskId, TStringBuilder() << VDisk.PrettyItemName() << " is locked by this request");
}

bool TDefaultErasureCounter::CheckForKeepAvailability(TClusterInfoPtr info, TErrorInfo &error,
        TInstant &defaultDeadline, bool allowPartial) const
{
    if (HasAlreadyLockedDisks && !HasMoreThanOneDiskPerNode && allowPartial) {
        CmsCounters->Cumulative()[COUNTER_PARTIAL_PERMISSIONS_OPTIMIZED].Increment(1);
        error.Code = TStatus::DISALLOW_TEMP;
        error.Reason = "You cannot get two or more disks from the same group at the same time"
                        " in partial permissions allowed mode";
        error.Deadline = defaultDeadline;
        return false;
    }

    if (Down.size() + Locked.size() > info->BSGroup(GroupId).Erasure.ParityParts()) {
        if (HasAlreadyLockedDisks && !allowPartial) {
            error.Code = TStatus::DISALLOW;
            error.Reason = "The request is incorrect: too many disks from the one group. "
                           "Fix the request or set PartialPermissionAllowed to true";
            return false;
        }

        error.Code = TStatus::DISALLOW_TEMP;
        error.Reason = TReason(
            TStringBuilder() << "Issue in affected group with id '" << GroupId << "'"
            << ": too many unavailable vdisks"
            << ". Locked: " << DumpVDisksInfo(Locked, info)
            << ". Down: " << DumpVDisksInfo(Down, info),
            TReason::EType::TooManyUnavailableVDisks
        );
        error.Deadline = defaultDeadline;
        return false;
    }

    return true;
}

bool TMirror3dcCounter::CheckForKeepAvailability(TClusterInfoPtr info, TErrorInfo &error,
        TInstant &defaultDeadline, bool allowPartial) const
{
    if (HasAlreadyLockedDisks && !HasMoreThanOneDiskPerNode && allowPartial) {
        CmsCounters->Cumulative()[COUNTER_PARTIAL_PERMISSIONS_OPTIMIZED].Increment(1);
        error.Code = TStatus::DISALLOW_TEMP;
        error.Reason = "You cannot get two or more disks from the same group at the same time"
                        " in partial permissions allowed mode";
        error.Deadline = defaultDeadline;
        return false;
    }

    if (DataCenterDisabledNodes.size() <= 1)
        return true;

    if (DataCenterDisabledNodes.size() == 2
        && (DataCenterDisabledNodes.begin()->second <= 1
            || (++DataCenterDisabledNodes.begin())->second <= 1))
    {
        return true;
    }

    if (HasAlreadyLockedDisks && !allowPartial) {
        error.Code = TStatus::DISALLOW;
        error.Reason = "The request is incorrect: too many disks from the one group. "
                       "Fix the request or set PartialPermissionAllowed to true";
        return false;
    }

    if (DataCenterDisabledNodes.size() > 2) {
        error.Code = TStatus::DISALLOW_TEMP;
        error.Reason = TReason(
            TStringBuilder() << "Issue in affected group with id '" << GroupId << "'"
            << ": too many unavailable vdisks"
            << ". Number of data centers with unavailable vdisks: " << DataCenterDisabledNodes.size()
            << ". Locked: " << DumpVDisksInfo(Locked, info)
            << ". Down: " << DumpVDisksInfo(Down, info),
            TReason::EType::TooManyUnavailableVDisks
        );
        error.Deadline = defaultDeadline;
        return false;
    }

    error.Code = TStatus::DISALLOW_TEMP;
    error.Reason = TReason(
        TStringBuilder() << "Issue in affected group with id '" << GroupId << "'"
        << ": too many unavailable vdisks"
        << ". Locked: " << DumpVDisksInfo(Locked, info)
        << ". Down: " << DumpVDisksInfo(Down, info),
        TReason::EType::TooManyUnavailableVDisks
    );
    error.Deadline = defaultDeadline;

    return false;
}

bool TMirror3dcCounter::CountVDisk(const TVDiskInfo &vdisk, TClusterInfoPtr info, TDuration retryTime,
        TDuration duration, TErrorInfo &error)
{
    const bool disabled = TErasureCounterBase::CountVDisk(vdisk, info, retryTime, duration, error);
    if (disabled) {
        ++DataCenterDisabledNodes[vdisk.VDiskId.FailRealm];
    }
    return disabled;
}

void TMirror3dcCounter::CountGroupState(TClusterInfoPtr info, TDuration retryTime, TDuration duration, TErrorInfo &error) {
    TErasureCounterBase::CountGroupState(info, retryTime, duration, error);
    ++DataCenterDisabledNodes[VDisk.VDiskId.FailRealm];
}

TSimpleSharedPtr<IErasureCounter> CreateErasureCounter(TErasureType::EErasureSpecies es,
     const TVDiskInfo &vdisk, ui32 groupId, TTabletCountersBase* cmsCounters) 
{
    switch (es) {
        case TErasureType::ErasureNone:
        case TErasureType::ErasureMirror3:
        case TErasureType::Erasure3Plus1Block:
        case TErasureType::Erasure3Plus1Stripe:
        case TErasureType::Erasure4Plus2Block:
        case TErasureType::Erasure3Plus2Block:
        case TErasureType::Erasure4Plus2Stripe:
        case TErasureType::Erasure3Plus2Stripe:
        case TErasureType::ErasureMirror3Plus2:
        case TErasureType::Erasure4Plus3Block:
        case TErasureType::Erasure4Plus3Stripe:
        case TErasureType::Erasure3Plus3Block:
        case TErasureType::Erasure3Plus3Stripe:
        case TErasureType::Erasure2Plus3Block:
        case TErasureType::Erasure2Plus3Stripe:
        case TErasureType::Erasure2Plus2Block:
        case TErasureType::Erasure2Plus2Stripe:
        case TErasureType::ErasureMirror3of4:
            return TSimpleSharedPtr<IErasureCounter>(new TDefaultErasureCounter(vdisk, groupId, cmsCounters));
        case TErasureType::ErasureMirror3dc:
            return TSimpleSharedPtr<IErasureCounter>(new TMirror3dcCounter(vdisk, groupId, cmsCounters));
        default:
            Y_ABORT("Unknown erasure type: %d", es);
    }
}

} // namespace NKikimr::NCms
