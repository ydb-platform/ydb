#include "erasure_checkers.h"

#include <ydb/core/protos/cms.pb.h>
#include <ydb/public/api/protos/draft/ydb_maintenance.pb.h>

#include <library/cpp/actors/core/log.h>

#include <util/string/cast.h>
#include <util/system/backtrace.h>
#include <util/system/yassert.h>

#include <bitset>
#include <sstream>
#include <vector>

namespace NKikimr::NCms {

using namespace Ydb::Maintenance;

IStorageGroupChecker::EVDiskState IStorageGroupChecker::VDiskState(NKikimrCms::EState state) {
    switch (state) {
        case NKikimrCms::UP:
            return VDISK_STATE_UP;
        case NKikimrCms::UNKNOWN:
            return VDISK_STATE_UNSPECIFIED;
        case NKikimrCms::DOWN:
            return VDISK_STATE_DOWN;
        case NKikimrCms::RESTART:
            return VDISK_STATE_RESTART;
        default:
            Y_FAIL("Unknown EState");
    }
}

TSimpleSharedPtr<IStorageGroupChecker> CreateStorageGroupChecker(TErasureType::EErasureSpecies es, ui32 groupId) {
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
            return TSimpleSharedPtr<IStorageGroupChecker>(new TDefaultErasureChecker(groupId));
        case TErasureType::ErasureMirror3dc:
            return TSimpleSharedPtr<IStorageGroupChecker>(new TMirror3dcChecker(groupId));
        default:
            Y_FAIL("Unknown erasure type: %d", es);
    }
}


void TErasureCheckerBase::AddVDisk(const TVDiskID& vdiskId) {
    if (DiskToState.contains(vdiskId)) {
        return;
    }
    DiskToState[vdiskId].State = VDISK_STATE_UNSPECIFIED;
}

void TErasureCheckerBase::UpdateVDisk(const TVDiskID& vdiskId, EState state) {
    AddVDisk(vdiskId);

    const auto newState = VDiskState(state);

    // The disk is marked based on the information obtained by InfoCollector.
    // If we marked the disk DOWN, it means that there was a reason
    if (DiskToState[vdiskId].State == VDISK_STATE_DOWN
        && newState == VDISK_STATE_UP)
        return;

    if (DiskToState[vdiskId].State == VDISK_STATE_DOWN) {
        --DownVDisksCount;
    }

    if (DiskToState[vdiskId].State == VDISK_STATE_LOCKED ||
        DiskToState[vdiskId].State == VDISK_STATE_RESTART) {
        --LockedVDisksCount;
    }

    DiskToState[vdiskId].State = newState;

    if (newState == VDISK_STATE_RESTART || newState == VDISK_STATE_LOCKED) {
        ++LockedVDisksCount;
    }

    if (newState == VDISK_STATE_DOWN) {
        ++DownVDisksCount;
    }
}

void TErasureCheckerBase::LockVDisk(const TVDiskID& vdiskId) {
    Y_VERIFY(DiskToState.contains(vdiskId));

    ++LockedVDisksCount;
    if (DiskToState[vdiskId].State == VDISK_STATE_DOWN) {
        DiskToState[vdiskId].State = VDISK_STATE_RESTART;
        --DownVDisksCount;
    } else {
        DiskToState[vdiskId].State = VDISK_STATE_LOCKED;
    }
}

void TErasureCheckerBase::UnlockVDisk(const TVDiskID& vdiskId) {
    Y_VERIFY(DiskToState.contains(vdiskId));

    --LockedVDisksCount;
    if (DiskToState[vdiskId].State == VDISK_STATE_RESTART) {
        DiskToState[vdiskId].State = VDISK_STATE_DOWN;
        ++DownVDisksCount;
    } else {
        DiskToState[vdiskId].State = VDISK_STATE_UP;
    }
}

void TErasureCheckerBase::EmplaceTask(const TVDiskID &vdiskId, i32 priority,
                                      ui64 order, const std::string &taskUId) {

    auto& priorities = DiskToState[vdiskId].Priorities;
    auto it = priorities.lower_bound(TVDiskState::TTaskPriority(priority, order, ""));

    if (it != priorities.end() && (it->Order == order && it->Priority == priority)) {
        if (it->TaskUId == taskUId) {
            return;
        }
        Y_FAIL("Task with the same priority and order already exists");
    } else {
        priorities.emplace_hint(it, priority, order, taskUId);
    }
}

void TErasureCheckerBase::RemoveTask(const std::string &taskUId) {
    auto taskUIdsEqual = [&taskUId](const TVDiskState::TTaskPriority &p) {
      return p.TaskUId == taskUId;
    };

    for (auto &[vdiskId, vdiskState] : DiskToState) {
            auto it = std::find_if(vdiskState.Priorities.begin(),
                                   vdiskState.Priorities.end(), taskUIdsEqual);

            if (it == vdiskState.Priorities.end()) {
                continue;
            }

            vdiskState.Priorities.erase(it);

    }
}

ActionState::ActionReason TDefaultErasureChecker::TryToLockVDisk(const TVDiskID &vdiskId, EAvailabilityMode mode, i32 priority, ui64 order) const {
    Y_VERIFY(DiskToState.contains(vdiskId));

    const auto& diskState = DiskToState.at(vdiskId);

    if (diskState.State == VDISK_STATE_RESTART
        || diskState.State == VDISK_STATE_LOCKED) {
        return ActionState::ACTION_REASON_ALREADY_LOCKED;
    }

    auto taskPriority = TVDiskState::TTaskPriority(priority, order, "");
    if (!diskState.Priorities.empty() && taskPriority < *diskState.Priorities.rbegin()) {
        return ActionState::ACTION_REASON_LOW_PRIORITY;
    }

    if (mode == NKikimrCms::MODE_FORCE_RESTART) {
        return ActionState::ACTION_REASON_OK;
    }

    // Check how many disks are waiting for higher prioriry task to be locked
    ui32 priorityLockedCount = 0;
    for (auto &[id, vdiskState] : DiskToState) {
        if (vdiskState.State != VDISK_STATE_UP) {
            continue;
        }

        if (!vdiskState.Priorities.empty() && taskPriority < *vdiskState.Priorities.rbegin()) {
            ++priorityLockedCount;
        }
    }

    ui32 disksLimit = 0;
    if (diskState.State == VDISK_STATE_DOWN) {
        disksLimit = 1;
    }

    switch (mode) {
        case NKikimrCms::MODE_MAX_AVAILABILITY:
            if ((LockedVDisksCount + DownVDisksCount + priorityLockedCount) > disksLimit) {
                    return ActionState::ACTION_REASON_TOO_MANY_UNAVAILABLE_VDISKS;
            }
            break;
        case NKikimrCms::MODE_KEEP_AVAILABLE:
            if ((LockedVDisksCount + DownVDisksCount + priorityLockedCount) >= disksLimit + 2) {
                return ActionState::ACTION_REASON_TOO_MANY_UNAVAILABLE_VDISKS;
            }
            break;
        default:
            Y_FAIL("Unexpected Availability mode");
    }

    return ActionState::ACTION_REASON_OK;
}

ActionState::ActionReason TMirror3dcChecker::TryToLockVDisk(const TVDiskID &vdiskId, EAvailabilityMode mode, i32 priority, ui64 order) const {
    Y_VERIFY(DiskToState.contains(vdiskId));

    const auto& diskState = DiskToState.at(vdiskId);
    const auto taskPriority = TVDiskState::TTaskPriority(priority, order, "");

    if (!diskState.Priorities.empty() && taskPriority < *diskState.Priorities.rbegin()) {
        return ActionState::ACTION_REASON_LOW_PRIORITY;
    }

    if (mode == MODE_FORCE_RESTART) {
        return ActionState::ACTION_REASON_OK;
    }

    if (diskState.State == VDISK_STATE_LOCKED
        || diskState.State == VDISK_STATE_RESTART) {
        return ActionState::ACTION_REASON_ALREADY_LOCKED;
    }

    const std::vector<std::bitset<9>> MaxOkGroups = {
        0x1E0, 0x1D0, 0x1C8, 0x1C4, 0x1C2, 0x1C1,
        0x138, 0xB8, 0x78, 0x3C, 0x3A, 0x39,
        0x107, 0x87, 0x47, 0x27, 0x17, 0xF,
    };

    ui32 priorityLockedCount = 0;
    std::bitset<9> groupState(0);
    for (auto& [id, state] : DiskToState) {
        if (id == vdiskId)
            continue;

        if (state.State != VDISK_STATE_UP
            || (!state.Priorities.empty() && taskPriority < *state.Priorities.rbegin())) {
            groupState |= (1 << (id.FailRealm * 3 + id.FailDomain));
        }

        if (!state.Priorities.empty() && taskPriority < *state.Priorities.rbegin()) {
            ++priorityLockedCount;
        }
    }
    groupState |= (1 << (vdiskId.FailRealm * 3 + vdiskId.FailDomain));

    ui32 downVDisks = diskState.State == VDISK_STATE_DOWN ? DownVDisksCount - 1 : DownVDisksCount;
    if (mode == NKikimrCms::MODE_MAX_AVAILABILITY) {
        if ((downVDisks + LockedVDisksCount + priorityLockedCount) == 0) {
            return ActionState::ACTION_REASON_OK;
        }
        return ActionState::ACTION_REASON_TOO_MANY_UNAVAILABLE_VDISKS;
    }

    size_t minCount = 9;
    for (auto okGroup : MaxOkGroups) {
        auto xoredState = (~okGroup) & groupState;
        minCount = std::min(minCount, xoredState.count());
    }

    if (minCount > 0) {
        return ActionState::ACTION_REASON_TOO_MANY_UNAVAILABLE_VDISKS;
    }

    return ActionState::ACTION_REASON_OK;
}

std::string TDefaultErasureChecker::ReadableReason(const TVDiskID &vdiskId,
                                       EAvailabilityMode mode, ActionState::ActionReason reason) const {
    std::stringstream readableReason;

    if (reason == ActionState::ACTION_REASON_OK) {
        readableReason << "Action is OK";
        return readableReason.str();
    }

    readableReason << "Cannot lock vdisk" << vdiskId.ToString() << ". ";

    switch (reason) {
        case ActionState::ACTION_REASON_TOO_MANY_UNAVAILABLE_VDISKS:
            readableReason << "Group " << GroupId
                << " has too many unavailable vdisks. "
                << "Down disks count: " << DownVDisksCount
                << ". Locked disks count: " << LockedVDisksCount;

            if (mode == NKikimrCms::MODE_KEEP_AVAILABLE) {
                readableReason << ". Limit of unavailable disks for mode " << NKikimrCms::EAvailabilityMode_Name(mode)
                               << " is " << 2;
            }
            if (mode == NKikimrCms::MODE_MAX_AVAILABILITY) {
                readableReason << ". Limit of unavailable disks for mode " << NKikimrCms::EAvailabilityMode_Name(mode)
                               << " is " << 1;
            }
            break;
        case ActionState::ACTION_REASON_ALREADY_LOCKED:
            // TODO:: add info about lock id
            readableReason << "Disk is already locked";
            break;
        case ActionState::ACTION_REASON_LOW_PRIORITY:
            // TODO:: add info about task with higher priority
            readableReason << "Task with higher priority in progress";
            break;
        default:
            Y_FAIL("Unexpected Reason");
    }

    return readableReason.str();
}

std::string TMirror3dcChecker::ReadableReason(const TVDiskID &vdiskId,
                                              EAvailabilityMode mode, ActionState::ActionReason reason) const {
    std::stringstream readableReason;

    if (reason == ActionState::ACTION_REASON_OK) {
        readableReason << "Action is OK";
        return readableReason.str();
    }

    readableReason << "Cannot lock vdisk" << vdiskId.ToString() << ". ";

    switch (reason) {
        case ActionState::ACTION_REASON_TOO_MANY_UNAVAILABLE_VDISKS:
            readableReason << "Group " << GroupId
                << " has too many unavailable vdisks. "
                << "Down disks count: " << DownVDisksCount
                << ". Locked disks count: " << LockedVDisksCount;

            if (mode == NKikimrCms::MODE_KEEP_AVAILABLE) {
                readableReason << ". Limit of unavailable disks for mode " << NKikimrCms::EAvailabilityMode_Name(mode)
                               << " is 1";
            }
            if (mode == NKikimrCms::MODE_MAX_AVAILABILITY) {
                readableReason << ". Limit of unavailable disks for mode " << NKikimrCms::EAvailabilityMode_Name(mode)
                               << "4, 3 of which are in the same data center";
            }
            break;
        case ActionState::ACTION_REASON_ALREADY_LOCKED:
            // TODO:: add info about lock id
            readableReason << "Disk is already locked";
            break;
        case ActionState::ACTION_REASON_LOW_PRIORITY:
            // TODO:: add info about task with higher priority
            readableReason << "Task with higher priority in progress";
            break;
        default:
            Y_FAIL("Unexpected Reason");
    }

    return readableReason.str();
}

} // namespace NKikimr::NCms
