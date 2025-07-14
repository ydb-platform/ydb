#pragma once
#include "defs.h"
#include "events.h"
#include <ydb/core/protos/statestorage.pb.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/library/actors/interconnect/event_filter.h>
#include <util/stream/str.h>
#include <util/generic/list.h>
#include <util/generic/map.h>

namespace NKikimr {

struct TEvStateStorage {
    enum EEv {
        // requests (local, to proxy)
        EvLookup = EventSpaceBegin(TKikimrEvents::ES_STATESTORAGE),
        EvUpdate,
        EvLock,
        EvResolveReplicas,
        EvRequestReplicasDumps,
        EvDelete,
        EvCleanup,
        EvResolveBoard,
        EvBoardInfo,
        EvResolveSchemeBoard, // subset (by hash)
        EvListSchemeBoard, // all
        EvUpdateGroupConfig,
        EvListStateStorage,
        EvBoardInfoUpdate,
        EvPublishActorGone,
        EvRingGroupPassAway,
        EvConfigVersionInfo,

        // replies (local, from proxy)
        EvInfo = EvLookup + 512,
        EvUpdateSignature,
        EvResolveReplicasList,
        EvResponseReplicasDumps,
        EvDeleteResult,
        EvListSchemeBoardResult,
        EvListStateStorageResult,

        // replicas interface
        EvReplicaLookup = EvLock + 2 * 512,
        EvReplicaUpdate,
        EvReplicaLock,
        EvReplicaLeaderDemoted,
        EvReplicaDumpRequest,
        EvReplicaDump,
        EvReplicaRegFollower,
        EvReplicaUnregFollower,
        EvReplicaDelete,
        EvReplicaCleanup,
        EvReplicaUpdateConfig,

        EvReplicaInfo = EvLock + 3 * 512,
        EvReplicaShutdown,

        EvReplicaBoardPublish = EvLock + 4 * 512,
        EvReplicaBoardLookup,
        EvReplicaBoardCleanup,
        EvReplicaBoardUnsubscribe,

        EvReplicaBoardPublishAck = EvLock + 5 * 512,
        EvReplicaBoardInfo,
        EvReplicaBoardInfoUpdate,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_STATESTORAGE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_STATESTORAGE)");

    struct TProxyOptions {
        enum ESigWaitMode {
            SigNone,
            SigAsync,
            SigSync,
        };

        ESigWaitMode SigWaitMode;

        TProxyOptions(ESigWaitMode sigWaitMode = SigNone)
            : SigWaitMode(sigWaitMode)
        {}

        TString ToString() const {
            switch (SigWaitMode) {
            case SigNone: return "SigNone";
            case SigAsync: return "SigAsync";
            case SigSync: return "SigSync";
            default: return "Unknown";
            }
        }
    };

    struct TEvLookup : public TEventLocal<TEvLookup, EvLookup> {
        const ui64 TabletID;
        const ui64 Cookie;
        const TProxyOptions ProxyOptions;

        TEvLookup(ui64 tabletId, ui64 cookie, const TProxyOptions &proxyOptions = TProxyOptions())
            : TabletID(tabletId)
            , Cookie(cookie)
            , ProxyOptions(proxyOptions)
        {}

        TEvLookup(const TEvLookup& ev)
            : TabletID(ev.TabletID)
            , Cookie(ev.Cookie)
            , ProxyOptions(ev.ProxyOptions)
        {}

        TString ToString() const {
            TStringStream str;
            str << "{EvLookup TabletID: " << TabletID;
            str << " Cookie: " << Cookie;
            str << " ProxyOptions: " << ProxyOptions.ToString();
            str << "}";
            return str.Str();
        }
    };

    class TSignature {
        THashMap<TActorId, ui64> ReplicasSignature;

    public:
        ui32 Size() const;
        bool HasReplicaSignature(const TActorId &replicaId) const;
        ui64 GetReplicaSignature(const TActorId &replicaId) const;
        void SetReplicaSignature(const TActorId &replicaId, ui64 signature);
        void Merge(const TEvStateStorage::TSignature& signature);
        TString ToString() const;
    };

    struct TEvUpdate : public TEventLocal<TEvUpdate, EvUpdate> {
        const ui64 TabletID;
        const ui64 Cookie;
        const TActorId ProposedLeader;
        const TActorId ProposedLeaderTablet;
        const ui32 ProposedGeneration;
        const ui32 ProposedStep;
        const TSignature Signature;
        const TProxyOptions ProxyOptions;

        TEvUpdate(ui64 tabletId, ui64 cookie, const TActorId &leader, const TActorId &leaderTablet, ui32 gen, ui32 step, const TSignature &sig, const TProxyOptions &proxyOptions = TProxyOptions())
            : TabletID(tabletId)
            , Cookie(cookie)
            , ProposedLeader(leader)
            , ProposedLeaderTablet(leaderTablet)
            , ProposedGeneration(gen)
            , ProposedStep(step)
            , Signature(sig)
            , ProxyOptions(proxyOptions)
        {
        }

        TEvUpdate(const TEvUpdate& ev)
            : TabletID(ev.TabletID)
            , Cookie(ev.Cookie)
            , ProposedLeader(ev.ProposedLeader)
            , ProposedLeaderTablet(ev.ProposedLeaderTablet)
            , ProposedGeneration(ev.ProposedGeneration)
            , ProposedStep(ev.ProposedStep)
            , Signature(ev.Signature)
            , ProxyOptions(ev.ProxyOptions)
        {
        }

        TString ToString() const {
            TStringStream str;
            str << "{EvUpdate TabletID: " << TabletID;
            str << " Cookie: " << Cookie;
            str << " ProposedLeader: " << ProposedLeader.ToString();
            str << " ProposedLeaderTablet: " << ProposedLeaderTablet.ToString();
            str << " ProposedGeneration: " << ProposedGeneration;
            str << " ProposedStep: " << ProposedStep;
            str << " Signature: " << Signature.ToString();
            str << " ProxyOptions: " << ProxyOptions.ToString();
            str << "}";
            return str.Str();
        }
    };

    struct TEvDelete : TEventLocal<TEvDelete, EvDelete> {
        const ui64 TabletID;
        const ui64 Cookie;

        TEvDelete(ui64 tabletId, ui64 cookie = 0)
            : TabletID(tabletId)
            , Cookie(cookie)
        {}

        TString ToString() const {
            TStringStream str;
            str << "{EvDelete TabletID: " << TabletID;
            str << " Cookie: " << Cookie;
            str << "}";
            return str.Str();
        }
    };

    struct TEvCleanup : TEventLocal<TEvCleanup, EvCleanup> {
        const ui64 TabletID;
        const TActorId ProposedLeader;

        TEvCleanup(ui64 tabletId, TActorId proposedLeader)
            : TabletID(tabletId)
            , ProposedLeader(proposedLeader)
        {}
    };

    struct TEvDeleteResult : TEventLocal<TEvDeleteResult, EvDeleteResult> {
        const ui64 TabletID;
        const NKikimrProto::EReplyStatus Status;

        TEvDeleteResult(ui64 tabletId, NKikimrProto::EReplyStatus status)
            : TabletID(tabletId)
            , Status(status)
        {}

        TString ToString() const {
            TStringStream str;
            str << "{EvDeleteResult TabletID: " << TabletID;
            str << " Status: " << (ui32)Status;
            str << "}";
            return str.Str();
        }
    };

    struct TEvLock : public TEventLocal<TEvLock, EvLock> {
        const ui64 TabletID;
        const ui64 Cookie;
        const TActorId ProposedLeader;
        const ui32 ProposedGeneration;
        const TSignature Signature;
        const TProxyOptions ProxyOptions;

        TEvLock(ui64 tabletId, ui64 cookie, const TActorId &leader, ui32 gen, const TSignature &sig, const TProxyOptions &proxyOptions = TProxyOptions())
            : TabletID(tabletId)
            , Cookie(cookie)
            , ProposedLeader(leader)
            , ProposedGeneration(gen)
            , Signature(sig)
            , ProxyOptions(proxyOptions)
        {
        }

        TEvLock(const TEvLock& ev)
            : TabletID(ev.TabletID)
            , Cookie(ev.Cookie)
            , ProposedLeader(ev.ProposedLeader)
            , ProposedGeneration(ev.ProposedGeneration)
            , Signature(ev.Signature)
            , ProxyOptions(ev.ProxyOptions)
        {
        }

        TString ToString() const {
            TStringStream str;
            str << "{EvLock TabletID: " << TabletID;
            str << " Cookie: " << Cookie;
            str << " ProposedLeader: " << ProposedLeader.ToString();
            str << " ProposedGeneration: " << ProposedGeneration;
            str << " Signature: " << Signature.ToString();
            str << " ProxyOptions: " << ProxyOptions.ToString();
            str << "}";
            return str.Str();
        }
    };

    struct TEvConfigVersionInfo : public TEventLocal<TEvConfigVersionInfo, EvConfigVersionInfo> {
        const ui64 ClusterStateGeneration;
        const ui64 ClusterStateGuid;

        TEvConfigVersionInfo(ui64 clusterStateGeneration, ui64 clusterStateGuid)
            : ClusterStateGeneration(clusterStateGeneration)
            , ClusterStateGuid(clusterStateGuid)
        {}
    };

    struct TEvInfo : public TEventLocal<TEvInfo, EvInfo> {
        const NKikimrProto::EReplyStatus Status;
        const ui64 TabletID;
        const ui64 Cookie;
        const TActorId CurrentLeader;
        const TActorId CurrentLeaderTablet;
        const ui32 CurrentGeneration;
        const ui32 CurrentStep;
        const bool Locked;
        const ui64 LockedFor;
        const TSignature Signature;
        TVector<std::pair<TActorId, TActorId>> Followers;

        TEvInfo(NKikimrProto::EReplyStatus status, ui64 tabletId, ui64 cookie, const TActorId &leader, const TActorId &leaderTablet, ui32 gen, ui32 step, bool locked, ui64 lockedFor, const TSignature &sig, const TMap<TActorId, TActorId> &followers)
            : Status(status)
            , TabletID(tabletId)
            , Cookie(cookie)
            , CurrentLeader(leader)
            , CurrentLeaderTablet(leaderTablet)
            , CurrentGeneration(gen)
            , CurrentStep(step)
            , Locked(locked)
            , LockedFor(lockedFor)
            , Signature(sig)
            , Followers(followers.begin(), followers.end())
        {}

        TString ToString() const {
            TStringStream str;
            str << "{EvInfo Status: " << (ui32)Status;
            str << " TabletID: " << TabletID;
            str << " Cookie: " << Cookie;
            str << " CurrentLeader: " << CurrentLeader.ToString();
            str << " CurrentLeaderTablet: " << CurrentLeaderTablet.ToString();
            str << " CurrentGeneration: " << CurrentGeneration;
            str << " CurrentStep: " << CurrentStep;
            str << " Locked: " << (Locked ? "true" : "false");
            str << " LockedFor: " << LockedFor;
            str << " Signature: " << Signature.ToString();
            if (!Followers.empty()) {
                str << " Followers: [";
                for (auto it = Followers.begin(); it != Followers.end(); ++it) {
                    if (it != Followers.begin()) {
                        str << ',';
                    }
                    str << '{' << it->first.ToString() << ',' << it->second.ToString() << '}';
                }
                str << "]";
            }
            str << "}";
            return str.Str();
        }
    };

    struct TEvUpdateSignature : public TEventLocal<TEvUpdateSignature, EvUpdateSignature> {
        const ui64 TabletID;
        const TSignature Signature;

        TEvUpdateSignature(ui64 tabletId, const TSignature &sig)
            : TabletID(tabletId)
            , Signature(sig)
        {}

        TString ToString() const {
            TStringStream str;
            str << "{EvUpdateSignature TabletID: " << TabletID;
            str << " Signature: " << Signature.ToString();
            str << "}";
            return str.Str();
        }
    };

    struct TEvReplicaLeaderDemoted : public TEventPB<TEvReplicaLeaderDemoted, NKikimrStateStorage::TEvReplicaLeaderDemoted, EvReplicaLeaderDemoted> {
        TEvReplicaLeaderDemoted() {}
        TEvReplicaLeaderDemoted(ui64 tabletId, ui64 signature)
        {
            Record.SetTabletID(tabletId);
            Record.SetSignature(signature);
        }
    };

    struct TEvResolveReplicas;
    struct TEvResolveBoard;
    struct TEvResolveSchemeBoard;
    struct TEvResolveReplicasList;
    struct TEvReplicaLookup;
    struct TEvReplicaInfo;
    struct TEvReplicaUpdate;
    struct TEvReplicaLock;
    struct TEvReplicaDelete;
    struct TEvReplicaCleanup;
    struct TEvReplicaBoardPublish;
    struct TEvReplicaBoardLookup;
    struct TEvReplicaBoardCleanup;
    struct TEvReplicaBoardUnsubscribe;
    struct TEvReplicaBoardPublishAck;
    struct TEvReplicaBoardInfo;
    struct TEvReplicaBoardInfoUpdate;
    struct TEvListSchemeBoard;
    struct TEvListSchemeBoardResult;
    struct TEvListStateStorage;
    struct TEvListStateStorageResult;
    struct TEvPublishActorGone;
    struct TEvUpdateGroupConfig;
    struct TEvRingGroupPassAway;

    struct TEvReplicaShutdown : public TEventPB<TEvStateStorage::TEvReplicaShutdown, NKikimrStateStorage::TEvReplicaShutdown, TEvStateStorage::EvReplicaShutdown> {
    };

    struct TEvReplicaDumpRequest : public TEventPB<TEvReplicaDumpRequest, NKikimrStateStorage::TEvDumpRequest, EvReplicaDumpRequest> {
    };

    struct TEvReplicaDump : public TEventPB<TEvReplicaDump, NKikimrStateStorage::TEvDump, EvReplicaDump> {
    };

    struct TEvRequestReplicasDumps : public TEventLocal<TEvRequestReplicasDumps, EvRequestReplicasDumps> {

    };

    struct TEvResponseReplicasDumps : public TEventLocal<TEvResponseReplicasDumps, EvResponseReplicasDumps> {
        TVector<std::pair<TActorId, TAutoPtr<TEvReplicaDump>>> ReplicasDumps;
    };

    struct TEvReplicaRegFollower : public TEventPB<TEvReplicaRegFollower, NKikimrStateStorage::TEvRegisterFollower, EvReplicaRegFollower> {
        TEvReplicaRegFollower()
        {}

        TEvReplicaRegFollower(ui64 tabletId, TActorId follower, TActorId tablet, bool isCandidate, ui64 clusterStateGeneration, ui64 clusterStateGuid)
        {
            Record.SetTabletID(tabletId);
            Record.SetClusterStateGeneration(clusterStateGeneration);
            Record.SetClusterStateGuid(clusterStateGuid);
            ActorIdToProto(follower, Record.MutableFollower());
            ActorIdToProto(tablet, Record.MutableFollowerTablet());
            Record.SetCandidate(isCandidate);
        }
    };

    struct TEvReplicaUnregFollower : public TEventPB<TEvReplicaUnregFollower, NKikimrStateStorage::TEvUnregisterFollower, EvReplicaUnregFollower> {
        TEvReplicaUnregFollower()
        {}

        TEvReplicaUnregFollower(ui64 tabletId, const TActorId &follower, ui64 clusterStateGeneration, ui64 clusterStateGuid)
        {
            Record.SetTabletID(tabletId);
            Record.SetClusterStateGeneration(clusterStateGeneration);
            Record.SetClusterStateGuid(clusterStateGuid);
            ActorIdToProto(follower, Record.MutableFollower());
        }
    };

    struct TBoardInfoEntry {
        TString Payload;
        bool Dropped = false;
    };

    struct TEvBoardInfo : public TEventLocal<TEvBoardInfo, EvBoardInfo> {
        enum class EStatus {
            Unknown,
            Ok,
            NotAvailable,
        };

        const EStatus Status;
        const TString Path;
        TMap<TActorId, TBoardInfoEntry> InfoEntries;

        TEvBoardInfo(EStatus status, const TString &path)
            : Status(status)
            , Path(path)
        {}

        TEvBoardInfo(const TEvBoardInfo &x)
            : Status(x.Status)
            , Path(x.Path)
            , InfoEntries(x.InfoEntries)
        {}
    };

    struct TEvBoardInfoUpdate : public TEventLocal<TEvBoardInfoUpdate, EvBoardInfoUpdate> {

        const TEvBoardInfo::EStatus Status;
        const TString Path;
        TMap<TActorId, TBoardInfoEntry> Updates;

        TEvBoardInfoUpdate(TEvBoardInfo::EStatus status, const TString &path)
            : Status(status)
            , Path(path)
        {}
    };
};

enum ERingGroupState {
    PRIMARY,
    SYNCHRONIZED,
    NOT_SYNCHRONIZED,
    DISCONNECTED
};

struct TStateStorageInfo : public TThrRefBase {
    struct TSelection {
        enum EStatus {
            StatusUnknown,
            StatusOk,
            StatusNoInfo,
            StatusOutdated,
            StatusUnavailable,
        };

        ui32 Sz;
        TArrayHolder<TActorId> SelectedReplicas;
        TArrayHolder<EStatus> Status;

        TSelection()
            : Sz(0)
        {}

        void MergeReply(EStatus status, EStatus *owner, ui64 targetCookie, bool resetOld);

        const TActorId* begin() const { return SelectedReplicas.Get(); }
        const TActorId* end() const { return SelectedReplicas.Get() + Sz; }
    };

    struct TRing {
        bool IsDisabled;
        bool UseRingSpecificNodeSelection;
        TVector<TActorId> Replicas;

        TActorId SelectReplica(ui32 hash) const;
        ui32 ContentHash() const;
    };

    struct TRingGroup {
        ERingGroupState State;
        bool WriteOnly = false;
        ui32 NToSelect = 0;
        TVector<TRing> Rings;

        TString ToString() const;
        bool SameConfiguration(const TStateStorageInfo::TRingGroup& rg);
    };

    TVector<TRingGroup> RingGroups;

    ui64 ClusterStateGeneration;
    ui64 ClusterStateGuid;
    ui32 StateStorageVersion;
    TVector<ui32> CompatibleVersions;

    void SelectReplicas(ui64 tabletId, TSelection *selection, ui32 ringGroupIdx) const;
    TList<TActorId> SelectAllReplicas() const;
    ui32 ContentHash() const;
    ui32 RingGroupsSelectionSize() const;

    TStateStorageInfo()
        : ClusterStateGeneration(0)
        , ClusterStateGuid(0)
        , StateStorageVersion(0)
        , Hash(Max<ui64>())
    {}

    TString ToString() const;

private:
    mutable ui64 Hash;
};

bool operator==(const TStateStorageInfo::TRing& lhs, const TStateStorageInfo::TRing& rhs);
bool operator==(const TStateStorageInfo::TRingGroup& lhs, const TStateStorageInfo::TRingGroup& rhs);
bool operator!=(const TStateStorageInfo::TRing& lhs, const TStateStorageInfo::TRing& rhs);
bool operator!=(const TStateStorageInfo::TRingGroup& lhs, const TStateStorageInfo::TRingGroup& rhs);

enum class EBoardLookupMode {
    First,
    Second,
    Majority,
    FirstNonEmptyDoubleTime,
    SecondNonEmptyDoubleTime,
    MajorityDoubleTime,
    Subscription,
};

struct TBoardRetrySettings {
    TDuration StartDelayMs = TDuration::MilliSeconds(2000);
    TDuration MaxDelayMs = TDuration::MilliSeconds(5000);
};

TIntrusivePtr<TStateStorageInfo> BuildStateStorageInfo(const NKikimrConfig::TDomainsConfig::TStateStorage& config);
TIntrusivePtr<TStateStorageInfo> BuildStateStorageBoardInfo(const NKikimrConfig::TDomainsConfig::TStateStorage& config);
TIntrusivePtr<TStateStorageInfo> BuildSchemeBoardInfo(const NKikimrConfig::TDomainsConfig::TStateStorage& config);
void BuildStateStorageInfos(const NKikimrConfig::TDomainsConfig::TStateStorage& config,
    TIntrusivePtr<TStateStorageInfo> &stateStorageInfo,
    TIntrusivePtr<TStateStorageInfo> &boardInfo,
    TIntrusivePtr<TStateStorageInfo> &schemeBoardInfo);

IActor* CreateStateStorageProxy(const TIntrusivePtr<TStateStorageInfo> &info, const TIntrusivePtr<TStateStorageInfo> &board, const TIntrusivePtr<TStateStorageInfo> &schemeBoard);
IActor* CreateStateStorageProxyStub();
IActor* CreateStateStorageReplica(const TIntrusivePtr<TStateStorageInfo> &info, ui32 replicaIndex);
IActor* CreateStateStorageMonitoringActor(ui64 targetTablet, const TActorId &sender, const TString &query);
IActor* CreateStateStorageTabletGuardian(ui64 tabletId, const TActorId &leader, const TActorId &tabletLeader, ui32 generation);
IActor* CreateStateStorageFollowerGuardian(ui64 tabletId, const TActorId &follower); // created as followerCandidate
IActor* CreateStateStorageBoardReplica(const TIntrusivePtr<TStateStorageInfo> &, ui32);
IActor* CreateSchemeBoardReplica(const TIntrusivePtr<TStateStorageInfo>&, ui32);
IActor* CreateBoardLookupActor(
    const TString &path, const TActorId &owner, EBoardLookupMode mode,
    TBoardRetrySettings boardRetrySettings = {}, ui64 cookie = 0);
IActor* CreateBoardPublishActor(
    const TString &path, const TString &payload, const TActorId &owner, ui32 ttlMs, bool reg,
    TBoardRetrySettings boardRetrySettings = {});

TString MakeEndpointsBoardPath(const TString &database);

void RegisterStateStorageEventScopes(const std::shared_ptr<TEventFilter>& filter);

}
