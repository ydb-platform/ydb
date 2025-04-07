#pragma once
#include "health_check_helper.h"
#include <ydb/core/base/hive.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/protos/blobstorage_distributed_config.pb.h>
#include <ydb/public/api/grpc/ydb_monitoring_v1.grpc.pb.h>
#include <library/cpp/digest/old_crc/crc.h>

namespace NKikimr::NHealthCheck {

using namespace NNodeWhiteboard;
using NNodeWhiteboard::TTabletId;

struct TTenantInfo {
    TString Name;
    Ydb::Cms::GetDatabaseStatusResult::State State;
};

struct TNodeTabletState {
    struct TTabletStateSettings {
        TInstant AliveBarrier;
        ui32 MaxRestartsPerPeriod; // per hour
        ui32 MaxTabletIdsStored = 10;
        bool ReportGoodTabletsIds = false;
    };

    enum class ETabletState {
        Good,
        Stopped,
        RestartsTooOften,
        Dead,
    };

    struct TNodeTabletStateCount {
        NKikimrTabletBase::TTabletTypes::EType Type;
        ETabletState State;
        bool Leader;
        int Count = 1;
        TStackVec<TString> Identifiers;

        static ETabletState GetState(const NKikimrHive::TTabletInfo& info, const TTabletStateSettings& settings) {
            if (info.volatilestate() == NKikimrHive::TABLET_VOLATILE_STATE_STOPPED) {
                return ETabletState::Stopped;
            }
            ETabletState state = (info.restartsperperiod() >= settings.MaxRestartsPerPeriod) ? ETabletState::RestartsTooOften : ETabletState::Good;
            if (info.volatilestate() == NKikimrHive::TABLET_VOLATILE_STATE_RUNNING) {
                return state;
            }
            if (info.tabletbootmode() != NKikimrHive::TABLET_BOOT_MODE_DEFAULT) {
                return state;
            }
            if (info.lastalivetimestamp() != 0 && TInstant::MilliSeconds(info.lastalivetimestamp()) < settings.AliveBarrier) {
                // Tablet is not alive for a long time
                // We should report it as dead unless it's just waiting to be created
                if (info.generation() == 0 && info.volatilestate() == NKikimrHive::TABLET_VOLATILE_STATE_BOOTING && !info.inwaitqueue()) {
                    return state;
                }
                return ETabletState::Dead;
            }
            return state;

        }

        TNodeTabletStateCount(const NKikimrHive::TTabletInfo& info, const TTabletStateSettings& settings)
            : Type(info.tablettype())
            , State(GetState(info, settings))
            , Leader(info.followerid() == 0)
        {
        }

        bool operator ==(const TNodeTabletStateCount& o) const {
            return State == o.State && Type == o.Type && Leader == o.Leader;
        }
    };

    TStackVec<TNodeTabletStateCount> Count;

    void AddTablet(const NKikimrHive::TTabletInfo& info, const TTabletStateSettings& settings) {
        TNodeTabletStateCount tabletState(info, settings);
        auto itCount = Find(Count, tabletState);
        if (itCount != Count.end()) {
            itCount->Count++;
        } else {
            Count.emplace_back(tabletState);
            itCount = std::prev(Count.end());
        }
        if (itCount->State != ETabletState::Good || settings.ReportGoodTabletsIds) {
            if (itCount->Identifiers.size() < settings.MaxTabletIdsStored) {
                TStringBuilder id;
                id << info.tabletid();
                if (info.followerid()) {
                    id << '.' << info.followerid();
                }
                itCount->Identifiers.emplace_back(id);
            }
        }
    }
};

struct TStoragePoolState {
    TString Name;
    THashSet<TGroupId> Groups;
};

struct TDatabaseState {
    TTabletId HiveId = {};
    TTabletId SchemeShardId = {};
    TPathId ResourcePathId = {};
    TVector<TNodeId> ComputeNodeIds;
    THashSet<ui64> StoragePools;        // BSConfig case
    THashSet<TString> StoragePoolNames; // no BSConfig case
    THashMap<std::pair<TTabletId, TFollowerId>, const NKikimrHive::TTabletInfo*> MergedTabletState;
    THashMap<TNodeId, TNodeTabletState> MergedNodeTabletState;
    THashMap<TNodeId, ui32> NodeRestartsPerPeriod;
    ui64 StorageQuota = 0;
    ui64 StorageUsage = 0;
    TMaybeServerlessComputeResourcesMode ServerlessComputeResourcesMode;
    TNodeId MaxTimeDifferenceNodeId = 0;
    TString Path;
};

struct TGroupState {
    TString ErasureSpecies;
    std::vector<const NKikimrSysView::TVSlotEntry*> VSlots;
    ui32 Generation;
    bool LayoutCorrect = true;
};

struct THintOverloadedShard {
    TTabletId TabletId;
    TFollowerId FollowerId;
    double CPUCores;
    TTabletId SchemeShardId;
    TString Message;
};

struct TTabletRequestsState {
    struct TTabletState {
        TTabletTypes::EType Type = TTabletTypes::Unknown;
        TString Database;
        bool IsUnresponsive = false;
        TDuration MaxResponseTime;
        TActorId TabletPipe = {};
    };

    struct TRequestState {
        TTabletId TabletId;
        TString Key;
        TMonotonic StartTime;
    };

    std::unordered_map<TTabletId, TTabletState> TabletStates;
    std::unordered_map<ui64, TRequestState> RequestsInFlight;
    ui64 RequestId = 0;

    // Some tablets (currently only BSC sys view) do not set response cookie
    // So we use special constant ids for those requests
    enum ERequestId : ui64 {
        RequestStoragePools = 1'000'000,
        RequestGroups,
        RequestVSlots,
        RequestPDisks,
        RequestGetPartitionStats,
    };

    void MakeRequest(TTabletId tabletId, const TString& key, ui64 requestId) {
        RequestsInFlight.emplace(requestId, TRequestState{tabletId, key, TMonotonic::Now()});
    }

    ui64 MakeRequest(TTabletId tabletId, const TString& key) {
        MakeRequest(tabletId, key, ++RequestId);
        return RequestId;
    }

    TTabletId CompleteRequest(ui64 requestId) {
        TTabletId tabletId = {};
        TMonotonic finishTime = TMonotonic::Now();
        auto itRequest = RequestsInFlight.find(requestId);
        if (itRequest != RequestsInFlight.end()) {
            TDuration responseTime = finishTime - itRequest->second.StartTime;
            tabletId = itRequest->second.TabletId;
            TTabletState& tabletState = TabletStates[tabletId];
            if (responseTime > tabletState.MaxResponseTime) {
                tabletState.MaxResponseTime = responseTime;
            }
            RequestsInFlight.erase(itRequest);
        }
        return tabletId;
    }
};

enum class ETags {
    None,
    DBState,
    StorageState,
    PoolState,
    GroupState,
    VDiskState,
    PDiskState,
    NodeState,
    VDiskSpace,
    PDiskSpace,
    ComputeState,
    TabletState,
    SystemTabletState,
    OverloadState,
    SyncState,
    Uptime,
    QuotaUsage,
};

struct TSelfCheckResult {
    struct TIssueRecord {
        Ydb::Monitoring::IssueLog IssueLog;
        ETags Tag;
    };

    Ydb::Monitoring::StatusFlag::Status OverallStatus = Ydb::Monitoring::StatusFlag::GREY;
    TList<TIssueRecord> IssueRecords;
    Ydb::Monitoring::Location Location;
    int Level;
    TString Type;
    TSelfCheckResult* Upper = nullptr;

    // first level
    TSelfCheckResult(const TString& type = "")
        : Level(1)
        , Type(type)
        , Upper(nullptr)
    {}

    TSelfCheckResult(TSelfCheckResult* upper, const TString& type = "")
        : Type(type)
        , Upper(upper)
    {
        if (Upper) {
            Location.CopyFrom(upper->Location);
            Level = upper->Level + 1;
        }
    }

    ~TSelfCheckResult() {
        if (Upper) {
            Upper->InheritFrom(*this);
        }
    }

    static bool IsErrorStatus(Ydb::Monitoring::StatusFlag::Status status) {
        return status != Ydb::Monitoring::StatusFlag::GREEN;
    }

    static TString crc16(const TString& data) {
        return Sprintf("%04x", (ui32)::crc16(data.data(), data.size()));
    }

    static TString crc32(const TString& data) {
        return Sprintf("%08x", (ui32)::crc32(data.data(), data.size()));
    }

    static TString GetIssueId(const Ydb::Monitoring::IssueLog& issueLog) {
        const Ydb::Monitoring::Location& location(issueLog.location());
        TStringStream id;
        if (issueLog.status() != Ydb::Monitoring::StatusFlag::UNSPECIFIED) {
            id << Ydb::Monitoring::StatusFlag_Status_Name(issueLog.status()) << '-';
        }
        id << crc16(issueLog.message());
        if (location.database().name()) {
            id << '-' << crc32(location.database().name());
        }
        if (location.storage().node().id()) {
            id << '-' << location.storage().node().id();
        } else {
            if (location.storage().node().host()) {
                id << '-' << location.storage().node().host();
            }
            if (location.storage().node().port()) {
                id << '-' << location.storage().node().port();
            }
        }
        if (!location.storage().pool().group().vdisk().id().empty()) {
            id << '-' << location.storage().pool().group().vdisk().id()[0];
        } else {
            if (!location.storage().pool().group().id().empty()) {
                id << '-' << location.storage().pool().group().id()[0];
            } else {
                if (location.storage().pool().name()) {
                    id << '-' << crc32(location.storage().pool().name());
                }
            }
        }
        if (!location.storage().pool().group().vdisk().pdisk().empty() && location.storage().pool().group().vdisk().pdisk()[0].id()) {
            id << '-' << location.storage().pool().group().vdisk().pdisk()[0].id();
        }
        if (location.compute().node().id()) {
            id << '-' << location.compute().node().id();
        } else {
            if (location.compute().node().host()) {
                id << '-' << location.compute().node().host();
            }
            if (location.compute().node().port()) {
                id << '-' << location.compute().node().port();
            }
        }
        if (location.compute().pool().name()) {
            id << '-' << location.compute().pool().name();
        }
        if (location.compute().tablet().type()) {
            id << '-' << location.compute().tablet().type();
        }
        if (location.compute().schema().path()) {
            id << '-' << crc32(location.compute().schema().path());
        }
        return id.Str();
    }

    void ReportStatus(Ydb::Monitoring::StatusFlag::Status status,
                        const TString& message = {},
                        ETags setTag = ETags::None,
                        std::initializer_list<ETags> includeTags = {}) {
        OverallStatus = MaxStatus(OverallStatus, status);
        if (IsErrorStatus(status)) {
            std::vector<TString> reason;
            if (includeTags.size() != 0) {
                for (const TIssueRecord& record : IssueRecords) {
                    for (const ETags& tag : includeTags) {
                        if (record.Tag == tag) {
                            reason.push_back(record.IssueLog.id());
                            break;
                        }
                    }
                }
            }
            std::sort(reason.begin(), reason.end());
            reason.erase(std::unique(reason.begin(), reason.end()), reason.end());
            TIssueRecord& issueRecord(*IssueRecords.emplace(IssueRecords.begin()));
            Ydb::Monitoring::IssueLog& issueLog(issueRecord.IssueLog);
            issueLog.set_status(status);
            issueLog.set_message(message);
            if (Location.ByteSizeLong() > 0) {
                issueLog.mutable_location()->CopyFrom(Location);
            }
            issueLog.set_id(GetIssueId(issueLog));
            if (Type) {
                issueLog.set_type(Type);
            }
            issueLog.set_level(Level);
            if (!reason.empty()) {
                for (const TString& r : reason) {
                    issueLog.add_reason(r);
                }
            }
            if (setTag != ETags::None) {
                issueRecord.Tag = setTag;
            }
        }
    }

    bool HasTags(std::initializer_list<ETags> tags) const {
        for (const TIssueRecord& record : IssueRecords) {
            for (const ETags tag : tags) {
                if (record.Tag == tag) {
                    return true;
                }
            }
        }
        return false;
    }

    Ydb::Monitoring::StatusFlag::Status FindMaxStatus(std::initializer_list<ETags> tags) const {
        Ydb::Monitoring::StatusFlag::Status status = Ydb::Monitoring::StatusFlag::GREY;
        for (const TIssueRecord& record : IssueRecords) {
            for (const ETags tag : tags) {
                if (record.Tag == tag) {
                    status = MaxStatus(status, record.IssueLog.status());
                }
            }
        }
        return status;
    }

    void ReportWithMaxChildStatus(const TString& message = {},
                                    ETags setTag = ETags::None,
                                    std::initializer_list<ETags> includeTags = {}) {
        if (HasTags(includeTags)) {
            ReportStatus(FindMaxStatus(includeTags), message, setTag, includeTags);
        }
    }

    Ydb::Monitoring::StatusFlag::Status GetOverallStatus() const {
        return OverallStatus;
    }

    void SetOverallStatus(Ydb::Monitoring::StatusFlag::Status status) {
        OverallStatus = status;
    }

    void InheritFrom(TSelfCheckResult& lower) {
        if (lower.GetOverallStatus() >= OverallStatus) {
            OverallStatus = lower.GetOverallStatus();
        }
        IssueRecords.splice(IssueRecords.end(), std::move(lower.IssueRecords));
    }
};
}; // NKikimr::NHealthCheck
