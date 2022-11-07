#pragma once

#include "defs.h"
#include "cms_state.h"
#include "pdiskid.h"
#include "pdisk_state.h"

#include <ydb/core/protos/blobstorage_config.pb.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/map.h>

namespace NKikimr {
namespace NCms {
namespace NSentinel {

using EPDiskStatus = NKikimrBlobStorage::EDriveStatus;
using TLimitsMap = TMap<EPDiskState, ui32>;

class TPDiskStatusComputer {
public:
    explicit TPDiskStatusComputer(const ui32& defaultStateLimit, const TLimitsMap& stateLimits);

    void AddState(EPDiskState state);
    EPDiskStatus Compute(EPDiskStatus current, TString& reason) const;

    EPDiskState GetState() const {
        return State;
    }

    EPDiskState GetPrevState() const {
        return PrevState;
    }

    ui64 GetStateCounter() const {
        return StateCounter;
    }

    void Reset();

private:
    const ui32& DefaultStateLimit;
    const TLimitsMap& StateLimits;

    EPDiskState State = NKikimrBlobStorage::TPDiskState::Unknown;
    mutable EPDiskState PrevState = State;
    ui64 StateCounter;

}; // TPDiskStatusComputer

class TPDiskStatus: public TPDiskStatusComputer {
public:
    explicit TPDiskStatus(EPDiskStatus initialStatus, const ui32& defaultStateLimit, const TLimitsMap& stateLimits);

    void AddState(EPDiskState state);
    bool IsChanged(TString& reason) const;
    bool IsChanged() const;
    void ApplyChanges(TString& reason);
    void ApplyChanges();
    EPDiskStatus GetStatus() const;
    bool IsNewStatusGood() const;

    bool IsChangingAllowed() const;
    void AllowChanging();
    void DisallowChanging();

private:
    EPDiskStatus Current;
    bool ChangingAllowed;

}; // TPDiskStatus

struct TStatusChangerState: public TSimpleRefCount<TStatusChangerState> {
    using TPtr = TIntrusivePtr<TStatusChangerState>;

    explicit TStatusChangerState(NKikimrBlobStorage::EDriveStatus status)
        : Status(status)
    {}

    const NKikimrBlobStorage::EDriveStatus Status;
    ui32 Attempt = 0;
}; // TStatusChangerState

struct TPDiskInfo
    : public TSimpleRefCount<TPDiskInfo>
    , public TPDiskStatus
{
    using TPtr = TIntrusivePtr<TPDiskInfo>;
    TActorId StatusChanger;
    TStatusChangerState::TPtr StatusChangerState;

    explicit TPDiskInfo(EPDiskStatus initialStatus, const ui32& defaultStateLimit, const TLimitsMap& stateLimits);

    bool IsTouched() const { return Touched; }
    void Touch() { Touched = true; }
    void ClearTouched() { Touched = false; }

    void AddState(EPDiskState state);

private:
    bool Touched;
}; // TPDiskInfo

class TClusterMap {
public:
    using TPDiskIDSet = THashSet<TPDiskID, TPDiskIDHash>;
    using TDistribution = THashMap<TString, TPDiskIDSet>;

    TCmsStatePtr State;
    TDistribution ByDataCenter;
    TDistribution ByRoom;
    TDistribution ByRack;

    TClusterMap(TCmsStatePtr state);

    void AddPDisk(const TPDiskID& id);
}; // TClusterMap

class TGuardian : public TClusterMap {
    static bool CheckRatio(ui32 check, ui32 base, ui32 ratio) {
        return (check * 100) <= (base * ratio);
    }

    static bool CheckRatio(const TDistribution::value_type& check, const TDistribution& base, ui32 ratio) {
        return CheckRatio(check.second.size(), base.at(check.first).size(), ratio);
    }

public:
    explicit TGuardian(TCmsStatePtr state, ui32 dataCenterRatio = 100, ui32 roomRatio = 100, ui32 rackRatio = 100);

    TPDiskIDSet GetAllowedPDisks(const TClusterMap& all, TString& issues, TPDiskIDSet& disallowed) const;

private:
    const ui32 DataCenterRatio;
    const ui32 RoomRatio;
    const ui32 RackRatio;
}; // TGuardian

} // NSentinel
} // NCms
} // NKikimr
