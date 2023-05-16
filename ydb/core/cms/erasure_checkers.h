#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/base/blobstorage_vdiskid.h>
#include <ydb/core/erasure/erasure.h>
#include <ydb/core/protos/cms.pb.h>
#include <ydb/public/api/protos/draft/ydb_maintenance.pb.h>

#include <util/generic/queue.h>
#include <util/system/compiler.h>

#include <algorithm>
#include <functional>
#include <queue>
#include <string>

namespace NKikimr::NCms {

using namespace NKikimrCms;

class IStorageGroupChecker {
public:
    enum EVDiskState : ui32 {
        VDISK_STATE_UNSPECIFIED /* "Unspecified" */,
        VDISK_STATE_UP /* "Up" */,
        VDISK_STATE_LOCKED /* "Locked" */,
        VDISK_STATE_RESTART /* "Restart" */,
        VDISK_STATE_DOWN /* "Down" */,
        VDISK_STATE_SCHEDULED_LOCKED /* "Scheduled locked" */
    };

protected:
    EVDiskState VDiskState(NKikimrCms::EState state);

public:
    virtual ~IStorageGroupChecker() = default;

    virtual void AddVDisk(const TVDiskID& vdiskId) = 0;
    virtual void UpdateVDisk(const TVDiskID& vdiskId, EState state) = 0;

    virtual void LockVDisk(const TVDiskID& vdiskId) = 0;
    virtual void UnlockVDisk(const TVDiskID& vdiskId) = 0;

    virtual void EmplaceTask(const TVDiskID& vdiskId, i32 priority, ui64 order, const std::string& taskUId) = 0;
    virtual void RemoveTask(const std::string& taskUId) = 0;

    virtual Ydb::Maintenance::ActionState::ActionReason TryToLockVDisk(const TVDiskID& vdiskId, EAvailabilityMode mode, i32 priority, ui64 order) const = 0;
    virtual std::string ReadableReason(const TVDiskID& vdiskId, EAvailabilityMode mode, Ydb::Maintenance::ActionState::ActionReason reason) const = 0;
};

class TErasureCheckerBase : public IStorageGroupChecker {
protected:
    /** Structure to hold information about vdisk state and priorities and orders of some task.
     *
     * Requests with equal priority are processed in the order of arrival at CMS. 
     */
    struct TVDiskState {
    public:
        struct TTaskPriority {
            i32 Priority;
            ui64 Order;
            std::string TaskUId;

            explicit TTaskPriority(i32 priority, ui64 order, const std::string& taskUId)
                : Priority(priority)
                , Order(order)
                , TaskUId(taskUId)
            {}

            bool operator<(const TTaskPriority& rhs) const {
                return Priority < rhs.Priority || (Priority == rhs.Priority && Order > rhs.Order);
            }
        };
    public:
        EVDiskState State;
        std::set<TTaskPriority> Priorities;
    };

protected:
    ui32 GroupId;

    THashMap<TVDiskID, TVDiskState> DiskToState;
    ui32 DownVDisksCount;
    ui32 LockedVDisksCount;

public:
    explicit TErasureCheckerBase(ui32 groupId)
        : GroupId(groupId)
        , DownVDisksCount(0)
        , LockedVDisksCount(0)
    {
    }
    virtual ~TErasureCheckerBase() = default;

    void AddVDisk(const TVDiskID& vdiskId) override final;
    void UpdateVDisk(const TVDiskID& vdiskId, EState state) override final;

    void LockVDisk(const TVDiskID& vdiskId) override final;
    void UnlockVDisk(const TVDiskID& vdiskId) override final;

    void EmplaceTask(const TVDiskID &vdiskId, i32 priority, ui64 order,
                     const std::string &taskUId) override final;
    void RemoveTask(const std::string &taskUId) override final;
};

class TDefaultErasureChecker : public TErasureCheckerBase {
public:
    explicit TDefaultErasureChecker(ui32 groupId)
        : TErasureCheckerBase(groupId)
    {}

    virtual Ydb::Maintenance::ActionState::ActionReason  TryToLockVDisk(const TVDiskID& vdiskId, EAvailabilityMode mode, i32 priority, ui64 order) const override final;
    
    virtual std::string ReadableReason(const TVDiskID &vdiskId,
                EAvailabilityMode mode, Ydb::Maintenance::ActionState::ActionReason reason) const override final;
};

class TMirror3dcChecker : public TErasureCheckerBase {
public:
    explicit TMirror3dcChecker(ui32 groupId)
        : TErasureCheckerBase(groupId)
    {}

    virtual Ydb::Maintenance::ActionState::ActionReason  TryToLockVDisk(const TVDiskID& vdiskId, EAvailabilityMode mode, i32 priority, ui64 order) const override final;

    virtual std::string ReadableReason(const TVDiskID &vdiskId,
                   EAvailabilityMode mode, Ydb::Maintenance::ActionState::ActionReason reason) const override final;
};

TSimpleSharedPtr<IStorageGroupChecker> CreateStorageGroupChecker(TErasureType::EErasureSpecies es, ui32 groupId);

} // namespace NKikimr::NCms
