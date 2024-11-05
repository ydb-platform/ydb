#pragma once
#include "defs.h"
#include <ydb/core/protos/local.pb.h>
#include <ydb/core/tablet/tablet_setup.h>
#include <ydb/core/base/events.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/subdomain.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/base/blobstorage.h>
#include <util/generic/map.h>
#include <ydb/core/tablet/tablet_metrics.h>

namespace NKikimr {

struct TRegistrationInfo {
    TString DomainName;
    TString TenantName;
    NKikimrTabletBase::TMetrics ResourceLimit;

    TRegistrationInfo(const TString &domainPath)
    {
        DomainName = ExtractDomain(domainPath);
        TenantName = domainPath;
    }

    TRegistrationInfo(const TString &domainName, NKikimrTabletBase::TMetrics resourceLimit)
        : TRegistrationInfo(domainName)
    {
        ResourceLimit.Swap(&resourceLimit);
    }

    bool TenantIsDomain() const {
        return IsEqualPaths(DomainName, TenantName);
    }

    TString GetDomain() const {
        return DomainName;
    }

    TString GetTenant() const {
        return TenantName;
    }
};


struct TDrainProgress : public TThrRefBase {
    std::atomic_uint64_t Events = 0;
    std::atomic_uint64_t OnlineTabletsEstimate = 0;
    std::atomic_bool ValidEstimate = false;

    bool CheckCompleted() {
        return Events.load() == 0;
    }

    std::optional<ui64> GetOnlineTabletsEstimate() {
        if (ValidEstimate.load()) {
            // A race here is safe as ValidEstimate will not become false after once becoming true
            return OnlineTabletsEstimate.load();
        } else {
            return std::nullopt;
        }
    }

    void UpdateEstimate(i64 diff) {
        OnlineTabletsEstimate += diff;
        if (diff != 0) {
            ValidEstimate = true;
        }
    }

    void OnSend() {
        ++Events;
    }

    void OnReceive() {
        --Events;
    }
};

struct TEvLocal {
    enum EEv {
        EvRegisterNode = EventSpaceBegin(TKikimrEvents::ES_LOCAL),
        EvPing,
        EvBootTablet,
        EvStopTablet, // must be here
        EvDeadTabletAck,
        EvEnumerateTablets,
        EvSyncTablets,
        EvTabletMetrics,
        EvReconnect,

        EvStatus = EvRegisterNode + 512,
        EvTabletStatus,
        EvEnumerateTabletsResult,
        EvTabletMetricsAck,

        EvAddTenant = EvRegisterNode + 1024,
        EvRemoveTenant,
        EvAlterTenant,
        EvTenantStatus,

        EvLocalDrainNode,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_LOCAL),
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_LOCAL)");

    struct TEvRegisterNode : public TEventPB<TEvRegisterNode, NKikimrLocal::TEvRegisterNode, EvRegisterNode> {
        TEvRegisterNode()
        {}

        TEvRegisterNode(ui64 hiveId) {
            Record.SetHiveId(hiveId);
        }
    };

    struct TEvPing : public TEventPB<TEvPing, NKikimrLocal::TEvPing, EvPing> {
        TEvPing()
        {}

        TEvPing(ui64 hiveId, ui32 hiveGeneration, bool purge, const NKikimrLocal::TLocalConfig &config) {
            Record.SetHiveId(hiveId);
            Record.SetHiveGeneration(hiveGeneration);
            Record.SetPurge(purge);
            Record.MutableConfig()->CopyFrom(config);
        }
    };

    struct TEvReconnect : TEventPB<TEvReconnect, NKikimrLocal::TEvReconnect, EvReconnect> {
        TEvReconnect() = default;

        TEvReconnect(ui64 hiveId, ui32 hiveGeneration) {
            Record.SetHiveId(hiveId);
            Record.SetHiveGeneration(hiveGeneration);
        }
    };

    struct TEvStatus : public TEventPB<TEvStatus, NKikimrLocal::TEvStatus, EvStatus> {
        enum EStatus {
            StatusOk,
            StatusOutdated,
            StatusDead,
        };

        TEvStatus() {
            Record.SetStatus(StatusOk);
        }

        TEvStatus(EStatus status) {
            Record.SetStatus(status);
        }

        TEvStatus(EStatus status, ui64 inbootTablets, ui64 onlineTablets, ui64 deadTablets) {
            Record.SetStatus(status);
            Record.SetInbootTablets(inbootTablets);
            Record.SetOnlineTablets(onlineTablets);
            Record.SetDeadTablets(deadTablets);
        }
    };

    struct TEvBootTablet : public TEventPB<TEvBootTablet, NKikimrLocal::TEvBootTablet, EvBootTablet> {
        TEvBootTablet()
        {}

        TEvBootTablet(const TTabletStorageInfo &info, ui32 followerId, ui32 suggestedGeneration) {
            TabletStorageInfoToProto(info, Record.MutableInfo());
            Record.SetSuggestedGeneration(suggestedGeneration);
            Record.SetBootMode(NKikimrLocal::BOOT_MODE_LEADER);
            Record.SetFollowerId(followerId);
        }

        TEvBootTablet(const TTabletStorageInfo &info, ui32 followerId) {
            TabletStorageInfoToProto(info, Record.MutableInfo());
            Record.SetBootMode(NKikimrLocal::BOOT_MODE_FOLLOWER);
            Record.SetFollowerId(followerId);
        }
    };

    struct TEvStopTablet : public TEventPB<TEvStopTablet, NKikimrLocal::TEvStopTablet, EvStopTablet> {
        TEvStopTablet()
        {}

        TEvStopTablet(std::pair<ui64, ui32> tabletId, ui32 generation = 0)
        {
            Record.SetTabletId(tabletId.first);
            Record.SetFollowerId(tabletId.second);
            Record.SetGeneration(generation);
        }
    };

    struct TEvDeadTabletAck : public TEventPB<TEvDeadTabletAck, NKikimrLocal::TEvDeadTabletAck, EvDeadTabletAck> {
        TEvDeadTabletAck()
        {}

        TEvDeadTabletAck(std::pair<ui64, ui32> tabletId, ui32 generation)
        {
            Record.SetTabletId(tabletId.first);
            Record.SetFollowerId(tabletId.second);
            Record.SetGeneration(generation);
        }
    };

    struct TEvTabletStatus : public TEventPB<TEvTabletStatus, NKikimrLocal::TEvTabletStatus, EvTabletStatus> {
        enum EStatus {
            StatusOk,
            StatusBootFailed,
            StatusTypeUnknown,
            StatusBSBootError,
            StatusBSWriteError,
            StatusFailed,
            StatusBootQueueUnknown,
            StatusSupersededByLeader,
        };

        TEvTabletStatus()
        {}

        TEvTabletStatus(EStatus status, TEvTablet::TEvTabletDead::EReason reason, std::pair<ui64, ui32> tabletId, ui32 generation) {
            Record.SetStatus(status);
            Record.SetReason(reason);
            Record.SetTabletID(tabletId.first);
            Record.SetFollowerId(tabletId.second);
            Record.SetGeneration(generation);
        }

        TEvTabletStatus(EStatus status, std::pair<ui64, ui32> tabletId, ui32 generation) {
            Record.SetStatus(status);
            Record.SetTabletID(tabletId.first);
            Record.SetFollowerId(tabletId.second);
            Record.SetGeneration(generation);
        }
    };


    struct TEvEnumerateTablets : public TEventPB<TEvEnumerateTablets, NKikimrLocal::TEvEnumerateTablets, EvEnumerateTablets> {
        TEvEnumerateTablets()
        {}

        TEvEnumerateTablets(TTabletTypes::EType tabletType) {
            Record.SetTabletType(tabletType);
        }
    };

    struct TEvEnumerateTabletsResult : public TEventPB<TEvEnumerateTabletsResult, NKikimrLocal::TEvEnumerateTabletsResult, EvEnumerateTabletsResult> {
        TEvEnumerateTabletsResult()
        {}

        TEvEnumerateTabletsResult(NKikimrProto::EReplyStatus status) {
            Record.SetStatus(status);
        }
    };

    struct TEvSyncTablets : public TEventPB<TEvSyncTablets, NKikimrLocal::TEvSyncTablets, EvSyncTablets> {
        TEvSyncTablets()
        {}
    };

    struct TEvTabletMetrics : public TEventLocal<TEvTabletMetrics, EvTabletMetrics> {
        ui64 TabletId;
        ui32 FollowerId;
        NKikimrTabletBase::TMetrics ResourceValues;

        TEvTabletMetrics(ui64 tabletId, ui32 followerId, const NKikimrTabletBase::TMetrics& resourceValues)
            : TabletId(tabletId)
            , FollowerId(followerId)
            , ResourceValues(resourceValues)
        {}
    };

    struct TEvTabletMetricsAck : public TEventPB<TEvTabletMetricsAck, NKikimrLocal::TEvTabletMetricsAck, EvTabletMetricsAck> {
    };

    struct TEvAddTenant : public TEventLocal<TEvAddTenant, EvAddTenant> {
        TRegistrationInfo TenantInfo;

        TEvAddTenant(const TString &domainName)
            : TenantInfo(domainName)
        {}

        TEvAddTenant(const TString &domainName, NKikimrTabletBase::TMetrics resourceLimit)
            : TenantInfo(domainName, resourceLimit)
        {}

        TEvAddTenant(const TRegistrationInfo &info)
            : TenantInfo(info)
        {}
    };

    struct TEvRemoveTenant : public TEventLocal<TEvRemoveTenant, EvRemoveTenant> {
        TString TenantName;

        TEvRemoveTenant(const TString &name)
            : TenantName(name)
        {}
    };

    struct TEvAlterTenant : public TEventLocal<TEvAlterTenant, EvAlterTenant> {
        TRegistrationInfo TenantInfo;

        TEvAlterTenant(const TString &domainName)
            : TenantInfo(domainName)
        {}

        TEvAlterTenant(const TString &domainName, NKikimrTabletBase::TMetrics resourceLimit)
            : TenantInfo(domainName, resourceLimit)
        {}

        TEvAlterTenant(const TRegistrationInfo &info)
            : TenantInfo(info)
        {}
    };

    struct TEvTenantStatus : public TEventLocal<TEvTenantStatus, EvTenantStatus> {
        enum EStatus {
            STARTED,
            STOPPED,
            UNKNOWN_TENANT
        };

        TString TenantName;
        EStatus Status;
        NKikimrTabletBase::TMetrics ResourceLimit;
        TString Error;
        THashMap<TString, TString> Attributes;
        TSubDomainKey DomainKey;

        TEvTenantStatus(const TString &tenant, EStatus status)
            : TenantName(tenant)
            , Status(status)
        {}

        TEvTenantStatus(const TString &tenant, EStatus status, const TString &error)
            : TenantName(tenant)
            , Status(status)
            , Error(error)
        {}

        TEvTenantStatus(const TString &tenant,
                        EStatus status,
                        NKikimrTabletBase::TMetrics resourceLimit,
                        const THashMap<TString, TString> &attrs,
                        const TSubDomainKey &domainKey)
            : TenantName(tenant)
            , Status(status)
            , ResourceLimit(resourceLimit)
            , Attributes(attrs)
            , DomainKey(domainKey)
        {}
    };

    struct TEvLocalDrainNode : public TEventLocal<TEvLocalDrainNode, EvLocalDrainNode> {
        TIntrusivePtr<TDrainProgress> DrainProgress;

        TEvLocalDrainNode(TIntrusivePtr<TDrainProgress> drainProgress)
            : DrainProgress(drainProgress)
        {}
    };
};

struct TLocalConfig : public TThrRefBase {
    using TPtr = TIntrusivePtr<TLocalConfig>;

    struct TTabletClassInfo {
        TTabletSetupInfo::TPtr SetupInfo;
        std::optional<ui64> MaxCount; // maximum allowed number of running tablets, nullopt means unlimited
        i32 Priority = 0;

        TTabletClassInfo()
        {}

        TTabletClassInfo(TTabletSetupInfo::TPtr setupInfo, i32 priority = 0)
            : SetupInfo(setupInfo)
            , Priority(priority)
        {}
    };

    TMap<TTabletTypes::EType, TTabletClassInfo> TabletClassInfo;
};

IActor* CreateLocal(TLocalConfig *config);

inline TActorId MakeLocalID(ui32 node) {
    char x[12] = { 'l', 'o', 'c', 'l'};
    x[4] = (char)(node & 0xFF);
    x[5] = (char)((node >> 8) & 0xFF);
    x[6] = (char)((node >> 16) & 0xFF);
    x[7] = (char)((node >> 24) & 0xFF);
    x[8] = 0;
    x[9] = 0;
    x[10] = 0;
    x[11] = 0;
    return TActorId(node, TStringBuf(x, 12));
}

inline TActorId MakeLocalRegistrarID(ui32 node, ui64 hiveId) {
    char x[12] = { 'l', 'o', 'c', 'l'};
    x[4] = (char)(node & 0xFF);
    x[5] = (char)((node >> 8) & 0xFF);
    x[6] = (char)((node >> 16) & 0xFF);
    x[7] = (char)((node >> 24) & 0xFF);
    x[8] = (char)((hiveId & 0xFF) ^ ((hiveId >> 32) & 0xFF));
    x[9] = (char)(((hiveId >> 8) & 0xFF) ^ ((hiveId >> 40) & 0xFF));
    x[10] = (char)(((hiveId >> 16) & 0xFF) ^ ((hiveId >> 48) & 0xFF));
    x[11] = (char)(((hiveId >> 24) & 0xFF) ^ ((hiveId >> 56) & 0xFF));
    return TActorId(node, TStringBuf(x, 12));
}

}
