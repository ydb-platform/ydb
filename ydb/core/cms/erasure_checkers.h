#pragma once

#include "defs.h"
#include "cluster_info.h"

#include <ydb/core/erasure/erasure.h>
#include <ydb/core/protos/cms.pb.h>

#include <util/generic/hash.h>

namespace NKikimr::NCms {

using namespace NKikimrCms;

class IErasureCounter {
protected:
    virtual bool CountVDisk(const TVDiskInfo& vdisk, TClusterInfoPtr info, TDuration retryTime, TDuration duration, TErrorInfo& error) = 0;

public:
    virtual ~IErasureCounter() = default;

    virtual bool GroupAlreadyHasLockedDisks() const = 0;
    virtual bool CheckForMaxAvailability(TClusterInfoPtr info, TErrorInfo& error, TInstant& defaultDeadline, bool allowPartial) const = 0;
    virtual bool CheckForKeepAvailability(TClusterInfoPtr info, TErrorInfo& error, TInstant& defaultDeadline, bool allowPartial) const = 0;
    virtual void CountGroupState(TClusterInfoPtr info, TDuration retryTime, TDuration duration, TErrorInfo& error) = 0;
};

class TErasureCounterBase: public IErasureCounter {
protected:
    // id & reason
    THashMap<TVDiskID, TString> Down;
    THashMap<TVDiskID, TString> Locked;
    const TVDiskInfo& VDisk;
    const ui32 GroupId;
    bool HasAlreadyLockedDisks;

protected:
    bool IsDown(const TVDiskInfo& vdisk, TClusterInfoPtr info, TDuration& retryTime, TErrorInfo& error);
    bool IsLocked(const TVDiskInfo& vdisk, TClusterInfoPtr info, TDuration& retryTime, TDuration& duration, TErrorInfo& error);
    bool CountVDisk(const TVDiskInfo& vdisk, TClusterInfoPtr info, TDuration retryTime, TDuration duration, TErrorInfo& error) override;

public:
    TErasureCounterBase(const TVDiskInfo& vdisk, ui32 groupId)
        : VDisk(vdisk)
        , GroupId(groupId)
        , HasAlreadyLockedDisks(false)
    {
    }

    bool GroupAlreadyHasLockedDisks() const final;
    bool CheckForMaxAvailability(TClusterInfoPtr info, TErrorInfo& error, TInstant& defaultDeadline, bool allowPartial) const final;
    void CountGroupState(TClusterInfoPtr info, TDuration retryTime, TDuration duration, TErrorInfo &error) override;
};

class TDefaultErasureCounter: public TErasureCounterBase {
public:
    TDefaultErasureCounter(const TVDiskInfo& vdisk, ui32 groupId)
        : TErasureCounterBase(vdisk, groupId)
    {
    }

    bool CheckForKeepAvailability(TClusterInfoPtr info, TErrorInfo& error, TInstant& defaultDeadline, bool allowPartial) const override;
};

class TMirror3dcCounter: public TErasureCounterBase {
    THashMap<ui8, ui32> DataCenterDisabledNodes;

protected:
    bool CountVDisk(const TVDiskInfo& vdisk, TClusterInfoPtr info, TDuration retryTime, TDuration duration, TErrorInfo& error) override;

public:
    TMirror3dcCounter(const TVDiskInfo& vdisk, ui32 groupId)
        : TErasureCounterBase(vdisk, groupId)
    {
    }

    bool CheckForKeepAvailability(TClusterInfoPtr info, TErrorInfo& error, TInstant& defaultDeadline, bool allowPartial) const override;
    void CountGroupState(TClusterInfoPtr info, TDuration retryTime, TDuration duration, TErrorInfo &error) override;
};

TSimpleSharedPtr<IErasureCounter> CreateErasureCounter(TErasureType::EErasureSpecies es, const TVDiskInfo& vdisk, ui32 groupId);

} // namespace NKikimr::NCms
