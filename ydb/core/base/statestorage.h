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

        TString ToString() const {
            TStringStream str;
            str << "{EvLookup TabletID: " << TabletID;
            str << " Cookie: " << Cookie;
            str << " ProxyOptions: " << ProxyOptions.ToString();
            str << "}";
            return str.Str();
        }
    };

    struct TEvUpdate : public TEventLocal<TEvUpdate, EvUpdate> {
        const ui64 TabletID;
        const ui64 Cookie;
        const TActorId ProposedLeader;
        const TActorId ProposedLeaderTablet;
        const ui32 ProposedGeneration;
        const ui32 ProposedStep;
        const ui32 SignatureSz;
        const TArrayHolder<ui64> Signature;
        const TProxyOptions ProxyOptions;

        TEvUpdate(ui64 tabletId, ui64 cookie, const TActorId &leader, const TActorId &leaderTablet, ui32 gen, ui32 step, const ui64 *sig, ui32 sigsz, const TProxyOptions &proxyOptions = TProxyOptions())
            : TabletID(tabletId)
            , Cookie(cookie)
            , ProposedLeader(leader)
            , ProposedLeaderTablet(leaderTablet)
            , ProposedGeneration(gen)
            , ProposedStep(step)
            , SignatureSz(sigsz)
            , Signature(new ui64[sigsz])
            , ProxyOptions(proxyOptions)
        {
            Copy(sig, sig + sigsz, Signature.Get());
        }

        TString ToString() const {
            TStringStream str;
            str << "{EvUpdate TabletID: " << TabletID;
            str << " Cookie: " << Cookie;
            str << " ProposedLeader: " << ProposedLeader.ToString();
            str << " ProposedLeaderTablet: " << ProposedLeaderTablet.ToString();
            str << " ProposedGeneration: " << ProposedGeneration;
            str << " ProposedStep: " << ProposedStep;
            str << " SignatureSz: " << SignatureSz;
            if (SignatureSz) {
                str << " Signature: {" << Signature[0];
                for (size_t i = 1; i < SignatureSz; ++i) {
                    str << ", " << Signature[i];
                }
                str << "}";
            }
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
        const ui32 SignatureSz;
        const TArrayHolder<ui64> Signature;
        const TProxyOptions ProxyOptions;

        TEvLock(ui64 tabletId, ui64 cookie, const TActorId &leader, ui32 gen, const ui64 *sig, ui32 sigsz, const TProxyOptions &proxyOptions = TProxyOptions())
            : TabletID(tabletId)
            , Cookie(cookie)
            , ProposedLeader(leader)
            , ProposedGeneration(gen)
            , SignatureSz(sigsz)
            , Signature(new ui64[sigsz])
            , ProxyOptions(proxyOptions)
        {
            Copy(sig, sig + sigsz, Signature.Get());
        }

        TString ToString() const {
            TStringStream str;
            str << "{EvLock TabletID: " << TabletID;
            str << " Cookie: " << Cookie;
            str << " ProposedLeader: " << ProposedLeader.ToString();
            str << " ProposedGeneration: " << ProposedGeneration;
            str << " SignatureSz: " << SignatureSz;
            if (SignatureSz) {
                str << " Signature: {" << Signature[0];
                for (size_t i = 1; i < SignatureSz; ++i) {
                    str << ", " << Signature[i];
                }
                str << "}";
            }
            str << " ProxyOptions: " << ProxyOptions.ToString();
            str << "}";
            return str.Str();
        }
    };

    inline static void MakeFilteredSignatureCopy(const ui64 *sig, ui32 sigsz, ui64 *target) {
        for (ui32 i = 0; i != sigsz; ++i) {
            if (sig[i] != Max<ui64>())
                target[i] = sig[i];
            else
                target[i] = 0;
        }
    }

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
        const ui32 SignatureSz;
        TArrayHolder<ui64> Signature;
        TVector<std::pair<TActorId, TActorId>> Followers;

        TEvInfo(NKikimrProto::EReplyStatus status, ui64 tabletId, ui64 cookie, const TActorId &leader, const TActorId &leaderTablet, ui32 gen, ui32 step, bool locked, ui64 lockedFor, const ui64 *sig, ui32 sigsz, const TMap<TActorId, TActorId> &followers)
            : Status(status)
            , TabletID(tabletId)
            , Cookie(cookie)
            , CurrentLeader(leader)
            , CurrentLeaderTablet(leaderTablet)
            , CurrentGeneration(gen)
            , CurrentStep(step)
            , Locked(locked)
            , LockedFor(lockedFor)
            , SignatureSz(sigsz)
            , Signature(new ui64[sigsz])
            , Followers(followers.begin(), followers.end())
        {
            MakeFilteredSignatureCopy(sig, sigsz, Signature.Get());
        }

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
            str << " SignatureSz: " << SignatureSz;
            if (SignatureSz) {
                str << " Signature: {" << Signature[0];
                for (size_t i = 1; i < SignatureSz; ++i) {
                    str << ", " << Signature[i];
                }
                str << "}";
            }
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
        const ui32 Sz;
        const TArrayHolder<ui64> Signature;

        TEvUpdateSignature(ui64 tabletId, ui64 *sig, ui32 sigsz)
            : TabletID(tabletId)
            , Sz(sigsz)
            , Signature(new ui64[sigsz])
        {
            MakeFilteredSignatureCopy(sig, sigsz, Signature.Get());
        }

        TString ToString() const {
            TStringStream str;
            str << "{EvUpdateSignature TabletID: " << TabletID;
            str << " Sz: " << Sz;
            if (Sz) {
                str << " Signature: {" << Signature[0];
                for (size_t i = 1; i < Sz; ++i) {
                    str << ", " << Signature[i];
                }
                str << "}";
            }
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

        TEvReplicaRegFollower(ui64 tabletId, TActorId follower, TActorId tablet, bool isCandidate)
        {
            Record.SetTabletID(tabletId);
            ActorIdToProto(follower, Record.MutableFollower());
            ActorIdToProto(tablet, Record.MutableFollowerTablet());
            Record.SetCandidate(isCandidate);
        }
    };

    struct TEvReplicaUnregFollower : public TEventPB<TEvReplicaUnregFollower, NKikimrStateStorage::TEvUnregisterFollower, EvReplicaUnregFollower> {
        TEvReplicaUnregFollower()
        {}

        TEvReplicaUnregFollower(ui64 tabletId, const TActorId &follower)
        {
            Record.SetTabletID(tabletId);
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

struct TStateStorageInfo : public TThrRefBase {
    struct TSelection {
        enum EStatus {
            StatusUnknown,
            StatusOk,
            StatusNoInfo,
            StatusOutdated,
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

    ui32 NToSelect;
    TVector<TRing> Rings;

    ui32 StateStorageVersion;
    TVector<ui32> CompatibleVersions;

    void SelectReplicas(ui64 tabletId, TSelection *selection) const;
    TList<TActorId> SelectAllReplicas() const;
    ui32 ContentHash() const;

    TStateStorageInfo()
        : NToSelect(0)
        , Hash(Max<ui64>())
    {}

    TString ToString() const;

private:
    mutable ui64 Hash;
};

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

TIntrusivePtr<TStateStorageInfo> BuildStateStorageInfo(char (&namePrefix)[TActorId::MaxServiceIDLength], const NKikimrConfig::TDomainsConfig::TStateStorage& config);
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
    TBoardRetrySettings boardRetrySettings = {});
IActor* CreateBoardPublishActor(
    const TString &path, const TString &payload, const TActorId &owner, ui32 ttlMs, bool reg,
    TBoardRetrySettings boardRetrySettings = {});

TString MakeEndpointsBoardPath(const TString &database);

void RegisterStateStorageEventScopes(const std::shared_ptr<TEventFilter>& filter);

}
