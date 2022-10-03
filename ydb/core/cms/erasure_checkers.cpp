#include "erasure_checkers.h"

namespace NKikimr {
namespace NCms {

bool TErasureCounterBase::IsDown(const TVDiskInfo& vdisk, 
                                 TClusterInfoPtr info,
                                 TDuration& retryTime, 
                                 TErrorInfo& error)
{
    const auto& node = info->Node(vdisk.NodeId);
    const auto& pdisk = info->PDisk(vdisk.PDiskId);
    const auto defaultDeadline = TActivationContext::Now() + retryTime;

    // Check we received info for PDisk.
    if (!pdisk.NodeId) {
        ++Down;
        error.Reason = TStringBuilder() << "Missing info for " << pdisk.ItemName();
        return false;
    }

    return (node.NodeId != VDisk.NodeId && node.IsDown(error, defaultDeadline))
        || (pdisk.PDiskId != VDisk.PDiskId && pdisk.IsDown(error, defaultDeadline))
        || vdisk.IsDown(error, defaultDeadline);
}

bool TErasureCounterBase::IsLocked(const TVDiskInfo& vdisk,
                                   TClusterInfoPtr info, 
                                   TDuration& retryTime,
                                   TDuration& duration, 
                                   TErrorInfo& error)
{
    const auto& node = info->Node(vdisk.NodeId);
    const auto& pdisk = info->PDisk(vdisk.PDiskId);

    // Check we received info for VDisk.
    if (!vdisk.NodeId || !vdisk.PDiskId) {
        ++Down;
        error.Code = TStatus::DISALLOW_TEMP;
        error.Reason = TStringBuilder() << "Missing info for " << vdisk.ItemName();
        return false;
    }

    return node.IsLocked(error, retryTime, TActivationContext::Now(), duration) 
        || pdisk.IsLocked(error, retryTime, TActivationContext::Now(), duration) 
        || vdisk.IsLocked(error, retryTime, TActivationContext::Now(), duration);
}

bool TErasureCounterBase::GroupAlreadyHasLockedDisks(TErrorInfo& error) const
{
    if (Locked && error.Code == TStatus::DISALLOW) {
        error.Reason = "Group already has locked disks";
        return true;
    }
    return false;
}

bool TErasureCounterBase::CheckForMaxAvailability(TErrorInfo& error, 
                                                  TInstant& defaultDeadline) const
{
    if (Locked + Down > 1) {
        error.Code = TStatus::DISALLOW_TEMP;
        error.Reason = TStringBuilder() << "Issue in affected group " << GroupId
                                        << ". " << "Too many locked and down vdisks: " << Locked + Down;

        error.Deadline = defaultDeadline;
        return false;
    }
    return true;
}

void TDefaultErasureCounter::CountVDisk(const TVDiskInfo& vdisk,
                                        TClusterInfoPtr info,
                                        TDuration retryTime,
                                        TDuration duration, 
                                        TErrorInfo& error) 
{
    Y_VERIFY_DEBUG(vdisk.VDiskId != VDisk.VDiskId);

    // Check locks.
    TErrorInfo err;
    if (IsLocked(vdisk, info, retryTime, duration, err)) {
        ++Locked;
        error.Code = err.Code;
        error.Reason = TStringBuilder() << "Issue in affected group " << GroupId 
                                        << ". " << err.Reason;
        error.Deadline = Max(error.Deadline, err.Deadline);
        return;
    }

    // Check if disk is down.
    if (IsDown(vdisk, info, retryTime, err)) {
        ++Down;
        error.Code = err.Code;
        error.Reason = TStringBuilder() << "Issue in affected group " << GroupId
                                        << ". " << err.Reason;
        error.Deadline = Max(error.Deadline, err.Deadline);
    }
}

bool TDefaultErasureCounter::CheckForKeepAvailability(TClusterInfoPtr info, 
                                                      TErrorInfo& error, 
                                                      TInstant& defaultDeadline) const
{
    if (Down + Locked > info->BSGroup(GroupId).Erasure.ParityParts()) {
        error.Code = TStatus::DISALLOW_TEMP;
        error.Deadline = defaultDeadline;
        return false;
    }
    return true;
}

bool TMirror3dcCounter::CheckForKeepAvailability(TClusterInfoPtr info, 
                                                 TErrorInfo& error, 
                                                 TInstant& defaultDeadline) const
{
    Y_UNUSED(info);

    if (DataCenterDisabledNodes.size() <= 1)
        return true;
    
    if (DataCenterDisabledNodes.size() == 2
        && (DataCenterDisabledNodes.begin()->second <= 1
            || (++DataCenterDisabledNodes.begin())->second <= 1)) 
    {
        return true;
    }

    if (DataCenterDisabledNodes.size() > 2) {
        error.Code = TStatus::DISALLOW_TEMP;
        error.Reason = TStringBuilder() << "Issue in affected group " << GroupId 
                                        << ". Too many data centers have unavailable vdisks: "
                                        << DataCenterDisabledNodes.size();
        error.Deadline = defaultDeadline;
        return false;
    }

    error.Code = TStatus::DISALLOW_TEMP;
    error.Reason = TStringBuilder() << "Issue in affected group " << GroupId 
                                    << ". Data centers have too many unavailable vdisks";
    error.Deadline = defaultDeadline;

    return false;
}

void TMirror3dcCounter::CountVDisk(const TVDiskInfo& vdisk,
                                   TClusterInfoPtr info, 
                                   TDuration retryTime,
                                   TDuration duration,
                                   TErrorInfo& error) 
{
    Y_VERIFY_DEBUG(vdisk.VDiskId != VDisk.VDiskId);

    // Check locks.
    TErrorInfo err;
    if (IsLocked(vdisk, info, retryTime, duration, err)
        || IsDown(vdisk, info, retryTime, err)) {
        error.Code = err.Code;
        error.Reason = TStringBuilder() << "Issue in affected group " << GroupId 
                                        << ". " << err.Reason;
        error.Deadline = Max(error.Deadline, err.Deadline);
        ++DataCenterDisabledNodes[vdisk.VDiskId.FailRealm];
    }
}

void TMirror3dcCounter::CountGroupState(TClusterInfoPtr info,
                                        TDuration retryTime, 
                                        TDuration duration,
                                        TErrorInfo &error) 
{
    for (const auto &vdId : info->BSGroup(GroupId).VDisks) {
        if (vdId != VDisk.VDiskId)
            CountVDisk(info->VDisk(vdId), info, retryTime, duration, error);
    }
    ++Locked;
    ++DataCenterDisabledNodes[VDisk.VDiskId.FailRealm];
}

void TDefaultErasureCounter::CountGroupState(TClusterInfoPtr info,
                                             TDuration retryTime,
                                             TDuration duration,
                                             TErrorInfo &error) 
{
    for (const auto &vdId : info->BSGroup(GroupId).VDisks) {
        if (vdId != VDisk.VDiskId)
            CountVDisk(info->VDisk(vdId), info, retryTime, duration, error);
    }
    ++Locked;
}

TSimpleSharedPtr<IErasureCounter> CreateErasureCounter(TErasureType::EErasureSpecies es, 
                                                       const TVDiskInfo& vdisk, ui32 groupId) 
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
            return TSimpleSharedPtr<IErasureCounter>(new TDefaultErasureCounter(vdisk, groupId));
        case TErasureType::ErasureMirror3dc:
            return TSimpleSharedPtr<IErasureCounter>(new TMirror3dcCounter(vdisk, groupId));
        default:
            Y_FAIL("Unknown erasure type: %d", es);
    }
}

} // namespace NCms
} // namespace NKikimr
