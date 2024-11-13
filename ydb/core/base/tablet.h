#pragma once
#include "blobstorage.h"
#include "defs.h"
#include "events.h"
#include "logoblob.h"
#include "shared_quota.h"

#include <ydb/core/base/resource_profile.h>
#include <ydb/core/protos/base.pb.h>
#include <ydb/core/protos/tablet.pb.h>
#include <ydb/core/protos/tablet_tx.pb.h>
#include <ydb/core/tablet/tablet_metrics.h>

#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>
#include <util/generic/deque.h>
#include <util/generic/vector.h>
#include <util/datetime/base.h>

#include <functional>

namespace NKikimr {

TIntrusivePtr<TTabletStorageInfo> TabletStorageInfoFromProto(const NKikimrTabletBase::TTabletStorageInfo &proto);
void TabletStorageInfoToProto(const TTabletStorageInfo &info, NKikimrTabletBase::TTabletStorageInfo *proto);

inline ui64 MakeGenStepPair(ui32 gen, ui32 step) {
    ui64 g = gen;
    ui64 s = step;
    return (g << 32ull) | s;
}

inline std::pair<ui32, ui32> ExpandGenStepPair(ui64 x) {
    ui32 g = (ui32)(x >> 32ull);
    ui32 s = (ui32)(x);
    return std::pair<ui32, ui32>(g, s);
}

struct TEvTablet {
    enum EEv {
        EvBoot = EventSpaceBegin(TKikimrEvents::ES_TABLET),
        EvRestored,
        EvCommitStatus,
        EvCommitResult,
        EvPing,
        EvDemoted,
        EvNewFollowerAttached,
        EvFBoot,
        EvFUpdate,
        EvUnused,
        EvFAuxUpdate,
        EvFollowerGcApplied, // from leader to user tablet when all known followers reported consumed gc barrier
        EvFollowerSyncComplete, // from leader to user tablet when all old followers are touched and synced
        EvCutTabletHistory,
        EvUpdateConfig,
        EvDropLease,
        EvReady,
        EvFollowerDetached, // from leader to user tablet when a follower is removed

        EvCommit = EvBoot + 512,
        EvAux,
        EvPong,
        EvPreCommit,
        EvTabletActive,
        EvPromoteToLeader,
        EvFGcAck, // from user tablet to follower
        EvLeaseDropped,

        EvTabletDead = EvBoot + 1024,
        EvFollowerUpdateState, // notifications to guardian
        EvFeatures, // from user tablet to sys tablet, notify on supported features
        EvTabletStop, // from local to sys tablet, from sys tablet to user tablet
        EvTabletStopped, // from user tablet to sys tablet, ready to die now

        EvReadLocalBase = EvBoot + 1536,
        EvReadLocalBaseResult,
        EvLocalMKQL,
        EvLocalMKQLResponse,
        EvLocalSchemeTx,
        EvLocalSchemeTxResponse,
        EvGetCounters,
        EvGetCountersResponse,
        EvLocalReadColumns,
        EvLocalReadColumnsResponse,

        // from outside to leader
        EvFollowerAttach = EvBoot + 2048,
        EvFollowerDetach,
        EvFollowerListRefresh, // from guardian to leader
        EvReserved_00,
        EvFollowerGcAck, // from follower to leader

        // from leader to follower
        EvFollowerUpdate = EvBoot + 2560,
        EvFollowerAuxUpdate,
        EvReserved_01,
        EvFollowerDisconnect,
        EvFollowerRefresh, // from leader to follower

        // utilitary
        EvCheckBlobstorageStatusResult = EvBoot + 3072,
        EvResetTabletResult,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TABLET), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TABLET)");

    struct TCommitMetadata {
        ui32 Key;
        TString Data;

        TCommitMetadata(ui32 key, TString data)
            : Key(key)
            , Data(std::move(data))
        { }
    };

    struct TDependencyGraph : public TThrRefBase {
        struct TEntry {
            std::pair<ui32, ui32> Id;
            bool IsSnapshot;
            TVector<TLogoBlobID> References;

            TVector<TLogoBlobID> GcDiscovered;
            TVector<TLogoBlobID> GcLeft;

            TString EmbeddedLogBody;
            TVector<TCommitMetadata> EmbeddedMetadata;

            TEntry(const std::pair<ui32, ui32> &id, TVector<TLogoBlobID> &&refs, bool isSnapshot,
                    TVector<TLogoBlobID> &&gcDiscovered, TVector<TLogoBlobID> &&gcLeft,
                    TVector<TCommitMetadata> &&metadata)
                : Id(id)
                , IsSnapshot(isSnapshot)
                , References(std::move(refs))
                , GcDiscovered(std::move(gcDiscovered))
                , GcLeft(std::move(gcLeft))
                , EmbeddedMetadata(std::move(metadata))
            { }

            TEntry(const std::pair<ui32, ui32> &id, TString embeddedLogBody,
                    TVector<TLogoBlobID> &&gcDiscovered, TVector<TLogoBlobID> &&gcLeft,
                    TVector<TCommitMetadata> &&metadata)
                : Id(id)
                , IsSnapshot(false)
                , GcDiscovered(std::move(gcDiscovered))
                , GcLeft(std::move(gcLeft))
                , EmbeddedLogBody(std::move(embeddedLogBody))
                , EmbeddedMetadata(std::move(metadata))
            { }
        };

        std::pair<ui32, ui32> Snapshot;
        TDeque<TEntry> Entries;

        TDependencyGraph(const std::pair<ui32, ui32> &snap)
            : Snapshot(snap)
        {}

        void Describe(IOutputStream &out) const noexcept {
            out
                << "Deps{" << Snapshot.first << ":" << Snapshot.second
                << " entries " << Entries.size() << "}";
        }

        TEntry& AddEntry(const std::pair<ui32, ui32> &id, TVector<TLogoBlobID> &&refs, bool isSnapshot,
                TVector<TLogoBlobID> &&gcDiscovered, TVector<TLogoBlobID> &&gcLeft,
                TVector<TCommitMetadata> &&metadata)
        {
            if (isSnapshot) {
                Snapshot = id;
                Entries.clear();
            }

            return Entries.emplace_back(id, std::move(refs), isSnapshot, std::move(gcDiscovered), std::move(gcLeft), std::move(metadata));
        }

        TEntry& AddEntry(const std::pair<ui32, ui32> &id, TString embeddedLogBody,
                TVector<TLogoBlobID> &&gcDiscovered, TVector<TLogoBlobID> &&gcLeft,
                TVector<TCommitMetadata> &&metadata)
        {
            return Entries.emplace_back(id, std::move(embeddedLogBody), std::move(gcDiscovered), std::move(gcLeft), std::move(metadata));
        }

        void Invalidate() {
            Snapshot = std::make_pair(Max<ui32>(), Max<ui32>());
            Entries.clear();
        }

        bool IsValid() const {
            return (Snapshot.first != Max<ui32>());
        }
    };

    struct TLogEntryReference : public TLogoBlob {
        std::optional<TEvBlobStorage::TEvPut::ETactic> Tactic;

        using TLogoBlob::TLogoBlob;
    };

    // dependency graph to boot tablet, not yet ready
    struct TEvBoot : public TEventLocal<TEvBoot, EvBoot> {
        const ui64 TabletID;
        const ui32 Generation;

        TIntrusivePtr<TDependencyGraph> DependencyGraph;

        const TActorId Launcher;
        TIntrusivePtr<TTabletStorageInfo> TabletStorageInfo;
        TResourceProfilesPtr ResourceProfiles;
        TSharedQuotaPtr TxCacheQuota;

        NMetrics::TTabletThroughputRawValue GroupReadBytes;
        NMetrics::TTabletIopsRawValue GroupReadOps;

        TEvBoot(
                ui64 tabletId,
                ui32 generation,
                TDependencyGraph *dependencyGraph,
                const TActorId& launcher,
                TIntrusivePtr<TTabletStorageInfo> info,
                TResourceProfilesPtr profiles = nullptr,
                TSharedQuotaPtr txCacheQuota = nullptr,
                NMetrics::TTabletThroughputRawValue&& read = NMetrics::TTabletThroughputRawValue(),
                NMetrics::TTabletIopsRawValue&& readOps = NMetrics::TTabletIopsRawValue())
            : TabletID(tabletId)
            , Generation(generation)
            , DependencyGraph(dependencyGraph)
            , Launcher(launcher)
            , TabletStorageInfo(info)
            , ResourceProfiles(profiles)
            , TxCacheQuota(txCacheQuota)
            , GroupReadBytes(std::move(read))
            , GroupReadOps(std::move(readOps))
        {}
    };

    // tablet is restored, but may not yet be ready to accept messages
    struct TEvRestored : public TEventLocal<TEvRestored, EvRestored> {
        const ui64 TabletID;
        const ui32 Generation;
        const TActorId UserTabletActor;
        const bool Follower;

        TEvRestored(ui64 tabletId, ui32 generation, const TActorId &userTabletActor, bool follower)
            : TabletID(tabletId)
            , Generation(generation)
            , UserTabletActor(userTabletActor)
            , Follower(follower)
        {}
    };

    // tablet is ready for operation
    struct TEvReady : public TEventLocal<TEvReady, EvReady> {
        const ui64 TabletID;
        const ui32 Generation;
        const TActorId UserTabletActor;

        TEvReady(ui64 tabletId, ui32 generation, const TActorId &userTabletActor)
            : TabletID(tabletId)
            , Generation(generation)
            , UserTabletActor(userTabletActor)
        {}
    };

    struct TEvNewFollowerAttached : public TEventLocal<TEvNewFollowerAttached, EvNewFollowerAttached> {
        const ui32 TotalFollowers;

        TEvNewFollowerAttached(ui32 totalFollowers)
            : TotalFollowers(totalFollowers)
        {}
    };

    struct TEvFollowerDetached : public TEventLocal<TEvFollowerDetached, EvFollowerDetached> {
        const ui32 TotalFollowers;

        TEvFollowerDetached(ui32 totalFollowers)
            : TotalFollowers(totalFollowers)
        {}
    };

    // tablet
    struct TCommitInfo {
        const ui64 TabletID;
        const ui32 Generation;
        const ui32 Step;
        const TVector<ui32> DependsOn;
        const bool IsSnapshot;
        bool IsTotalSnapshot;
        bool WaitFollowerGcAck;

        TCommitInfo(ui64 tabletId, ui32 gen, ui32 step, const TVector<ui32> &dependsOn, bool isSnapshot)
            : TabletID(tabletId)
            , Generation(gen)
            , Step(step)
            , DependsOn(dependsOn)
            , IsSnapshot(isSnapshot)
            , IsTotalSnapshot(false)
            , WaitFollowerGcAck(false)
        {}
    };

    struct TEvCommit : public TEventLocal<TEvCommit, EvCommit>, public TCommitInfo {
        const bool PreCommited;
        TEvBlobStorage::TEvPut::ETactic CommitTactic;

        TVector<TLogoBlobID> ExternalReferences;
        TVector<TLogEntryReference> References;

        TVector<TLogoBlobID> GcDiscovered;
        TVector<TLogoBlobID> GcLeft;

        TString EmbeddedLogBody;
        TString FollowerAux;

        TVector<TCommitMetadata> EmbeddedMetadata;

        TEvCommit(ui64 tabletId, ui32 gen, ui32 step, const TVector<ui32> &dependsOn, bool isSnapshot
                , bool preCommited = false
                , TEvBlobStorage::TEvPut::ETactic tactic = TEvBlobStorage::TEvPut::TacticMinLatency)
            : TCommitInfo(tabletId, gen, step, dependsOn, isSnapshot)
            , PreCommited(preCommited)
            , CommitTactic(tactic)
        {}
    };

    struct TEvAux : public TEventLocal<TEvAux, EvAux> {
        TString FollowerAux;

        TEvAux(TString followerAux)
            : FollowerAux(std::move(followerAux))
        {}
    };

    struct TEvPreCommit : public TEventLocal<TEvPreCommit, EvPreCommit>, public TCommitInfo {
        TEvPreCommit(ui64 tabletId, ui32 gen, ui32 step, const TVector<ui32> &dependsOn, bool isSnapshot)
            : TCommitInfo(tabletId, gen, step, dependsOn, isSnapshot)
        {}
    };

    struct TEvTabletActive : public TEventLocal<TEvTabletActive, EvTabletActive> {};

    struct TEvDemoted : public TEventLocal<TEvDemoted, EvDemoted> {
        const bool ByIsolation;

        TEvDemoted(bool byIsolation)
            : ByIsolation(byIsolation)
        {}
    };

    struct TEvPromoteToLeader : public TEventLocal<TEvPromoteToLeader, EvPromoteToLeader> {
        const ui32 SuggestedGeneration;
        TIntrusivePtr<TTabletStorageInfo> TabletStorageInfo;

        TEvPromoteToLeader(ui32 suggestedGeneration, TIntrusivePtr<TTabletStorageInfo> info)
            : SuggestedGeneration(suggestedGeneration)
            , TabletStorageInfo(info)
        {}
    };

    struct TEvCommitResult : public TEventLocal<TEvCommitResult, EvCommitResult> {
        const NKikimrProto::EReplyStatus Status;
        const ui64 TabletID;
        const ui32 Generation;
        const ui32 Step;
        const ui32 ConfirmedOnSend;
        TVector<ui32> YellowMoveChannels;
        TVector<ui32> YellowStopChannels;
        THashMap<ui32, float> ApproximateFreeSpaceShareByChannel;
        NMetrics::TTabletThroughputRawValue GroupWrittenBytes;
        NMetrics::TTabletIopsRawValue GroupWrittenOps;

        TEvCommitResult(
                NKikimrProto::EReplyStatus status,
                ui64 tabletId,
                ui32 gen,
                ui32 step,
                ui32 confirmedOnSend,
                TVector<ui32>&& yellowMoveChannels,
                TVector<ui32>&& yellowStopChannels,
                THashMap<ui32, float>&& approximateFreeSpaceShareByChannel,
                NMetrics::TTabletThroughputRawValue&& written,
                NMetrics::TTabletIopsRawValue&& writtenOps)
            : Status(status)
            , TabletID(tabletId)
            , Generation(gen)
            , Step(step)
            , ConfirmedOnSend(confirmedOnSend)
            , YellowMoveChannels(std::move(yellowMoveChannels))
            , YellowStopChannels(std::move(yellowStopChannels))
            , ApproximateFreeSpaceShareByChannel(std::move(approximateFreeSpaceShareByChannel))
            , GroupWrittenBytes(std::move(written))
            , GroupWrittenOps(std::move(writtenOps))
        {}
    };

    struct TEvTabletDead : public TEventLocal<TEvTabletDead, EvTabletDead> {
#define TABLET_DEAD_REASON_MAP(XX) \
        XX(ReasonBootLocked, 0) \
        XX(ReasonBootSSTimeout, 1) \
        XX(ReasonBootRace, 2) \
        XX(ReasonReserved01, 3) \
        XX(ReasonBootBSError, 4) \
        XX(ReasonBootSuggestOutdated, 5) \
        XX(ReasonBootSSError, 6) \
        XX(ReasonBootReservedValue, 32) \
        XX(ReasonPill, 33) \
        XX(ReasonError, 34) \
        XX(ReasonDemotedByStateStorage, 35) \
        XX(ReasonReserved03, 36) \
        XX(ReasonBSError, 37) \
        XX(ReasonInconsistentCommit, 38) \
        XX(ReasonIsolated, 39) \
        XX(ReasonDemotedByBlobStorage, 40)

        enum EReason {
            TABLET_DEAD_REASON_MAP(ENUM_VALUE_GEN)
        };

        static void Out(IOutputStream& o, EReason x);
        static const char* Str(EReason status);

        const ui64 TabletID;
        const EReason Reason;
        const ui32 Generation;

        bool IsBootReason() const {
            return (Reason <= ReasonBootReservedValue);
        }

        TEvTabletDead(ui64 tabletId, EReason reason, ui32 generation)
            : TabletID(tabletId)
            , Reason(reason)
            , Generation(generation)
        {}
    };

    struct TEvFollowerUpdateState : public TEventLocal<TEvFollowerUpdateState, EvFollowerUpdateState> {
        const bool IsCandidate;
        const TActorId FollowerActor;
        const TActorId TabletActor;

        TEvFollowerUpdateState(bool isCandidate, TActorId followerActor, TActorId tabletActor)
            : IsCandidate(isCandidate)
            , FollowerActor(followerActor)
            , TabletActor(tabletActor)
        {}
    };

    struct TEvPing : public TEventPB<TEvPing, NKikimrTabletBase::TEvPing, EvPing> {
        enum EFlags {
            FlagWaitBoot = 1,
        };

        TEvPing()
        {}

        TEvPing(ui64 tabletId, ui64 flags)
        {
            Record.SetTabletID(tabletId);
            Record.SetFlags(flags);
        }
    };

    struct TEvPong : public TEventPB<TEvPong, NKikimrTabletBase::TEvPong, EvPong> {
        enum EFlags {
            FlagBoot = 1,
            FlagLeader = 2,
            FlagFollower = 4,
        };

        TEvPong()
        {}

        TEvPong(ui64 tabletId, ui64 flags)
        {
            Record.SetTabletID(tabletId);
            Record.SetFlags(flags);
        }
    };

    struct TEvReadLocalBase : public TEventPB<TEvReadLocalBase, NKikimrTabletBase::TEvReadLocalBase, EvReadLocalBase> {
        TEvReadLocalBase()
        {}

        TEvReadLocalBase(const TString& rootKey, bool saveScheme)
        {
            if (!rootKey.empty())
                Record.SetRootKey(rootKey);
            if (saveScheme)
                Record.SetSaveScheme(true);
        }
    };

    struct TEvReadLocalBaseResult : public TEventPB<TEvReadLocalBaseResult, NKikimrTabletBase::TEvReadLocalBaseResult, TEvTablet::EvReadLocalBaseResult> {

        ui64 Origin() const { return Record.GetOrigin(); }
        bool IsError() const { return Record.HasIsError() && Record.GetIsError(); }
        TStringBuf DocBuffer() const { return Record.HasDocBuffer() ? Record.GetDocBuffer() : TStringBuf(); }

        TEvReadLocalBaseResult()
        {}

        TEvReadLocalBaseResult(ui64 origin, const TString& rootKey, bool isError, const TString& docBuffer, const TString* schemeBuffer)
        {
            Record.SetOrigin(origin);
            Record.SetRootKey(rootKey);
            Record.SetIsError(isError);
            Record.SetDocBuffer(docBuffer);
            if (schemeBuffer)
                Record.SetScheme(*schemeBuffer);
        }
    };

    struct TEvLocalMKQL : public TEventPB<TEvLocalMKQL, NKikimrTabletTxBase::TEvLocalMKQL, TEvTablet::EvLocalMKQL> {
        TEvLocalMKQL()
        {}
    };

    struct TEvLocalMKQLResponse : public TEventPB<TEvLocalMKQLResponse, NKikimrTabletTxBase::TEvLocalMKQLResponse, TEvTablet::EvLocalMKQLResponse> {
        TEvLocalMKQLResponse()
        {}
    };

    struct TEvLocalSchemeTx : public TEventPB<TEvLocalSchemeTx, NKikimrTabletTxBase::TEvLocalSchemeTx, TEvTablet::EvLocalSchemeTx> {
        TEvLocalSchemeTx()
        {}
    };

    struct TEvLocalSchemeTxResponse : public TEventPB<TEvLocalSchemeTxResponse, NKikimrTabletTxBase::TEvLocalSchemeTxResponse, TEvTablet::EvLocalSchemeTxResponse> {
        TEvLocalSchemeTxResponse()
        {}
    };

    struct TEvLocalReadColumns : public TEventPB<TEvLocalReadColumns, NKikimrTabletTxBase::TEvLocalReadColumns, TEvTablet::EvLocalReadColumns> {
        TEvLocalReadColumns()
        {}
    };

    struct TEvLocalReadColumnsResponse : public TEventPB<TEvLocalReadColumnsResponse, NKikimrTabletTxBase::TEvLocalReadColumnsResponse, TEvTablet::EvLocalReadColumnsResponse> {
        TEvLocalReadColumnsResponse()
        {}
    };

    struct TEvFollowerAttach : public TEventPB<TEvFollowerAttach, NKikimrTabletBase::TEvFollowerAttach, EvFollowerAttach> {
        TEvFollowerAttach()
        {}

        TEvFollowerAttach(ui64 tabletId, ui32 followerAttempt)
        {
            Record.SetTabletId(tabletId);
            Record.SetFollowerAttempt(followerAttempt);
        }
    };

    struct TEvFollowerUpdate : public TEventPB<TEvFollowerUpdate, NKikimrTabletBase::TEvFollowerUpdate, EvFollowerUpdate> {
        TEvFollowerUpdate()
        {}

        TEvFollowerUpdate(ui64 tabletId, ui32 followerAttempt, ui64 streamCounter)
        {
            Record.SetTabletId(tabletId);
            Record.SetFollowerAttempt(followerAttempt);
            Record.SetStreamCounter(streamCounter);
        }
    };

    struct TEvFollowerAuxUpdate : public TEventPB<TEvFollowerAuxUpdate, NKikimrTabletBase::TEvFollowerAuxUpdate, EvFollowerAuxUpdate> {
        TEvFollowerAuxUpdate() = default;

        TEvFollowerAuxUpdate(ui64 tabletId, ui32 followerAttempt, ui64 streamCounter)
        {
            Record.SetTabletId(tabletId);
            Record.SetFollowerAttempt(followerAttempt);
            Record.SetStreamCounter(streamCounter);
        }
    };

    struct TEvFollowerDetach : public TEventPB<TEvFollowerDetach, NKikimrTabletBase::TEvFollowerDetach, EvFollowerDetach> {
        TEvFollowerDetach()
        {}

        TEvFollowerDetach(ui64 tabletId, ui32 followerAttempt)
        {
            Record.SetTabletId(tabletId);
            Record.SetFollowerAttempt(followerAttempt);
        }
    };

    struct TEvFollowerListRefresh : public TEventLocal<TEvFollowerListRefresh, EvFollowerListRefresh> {
        TVector<TActorId> FollowerList;

        TEvFollowerListRefresh(TVector<TActorId> &&followers)
            : FollowerList(std::move(followers))
        {}
    };

    struct TEvFollowerDisconnect : public TEventPB<TEvFollowerDisconnect, NKikimrTabletBase::TEvFollowerDisconnect, EvFollowerDisconnect> {
        TEvFollowerDisconnect()
        {}

        TEvFollowerDisconnect(ui64 tabletId, ui32 followerAttempt)
        {
            Record.SetTabletId(tabletId);
            Record.SetFollowerAttempt(followerAttempt);
        }
    };

    struct TEvFollowerRefresh : public TEventPB<TEvFollowerRefresh, NKikimrTabletBase::TEvFollowerRefresh, EvFollowerRefresh> {
        TEvFollowerRefresh()
        {}

        TEvFollowerRefresh(ui64 tabletId, ui32 generation)
        {
            Record.SetTabletId(tabletId);
            Record.SetGeneration(generation);
        }
    };

    struct TEvFollowerGcAck : public TEventPB<TEvFollowerGcAck, NKikimrTabletBase::TEvFollowerGcAck, EvFollowerGcAck> {
        TEvFollowerGcAck()
        {}

        TEvFollowerGcAck(ui64 tabletId, ui32 followerAttempt, ui32 generation, ui32 step)
        {
            Record.SetTabletId(tabletId);
            Record.SetFollowerAttempt(followerAttempt);
            Record.SetGeneration(generation);
            Record.SetStep(step);
        }
    };

    struct TFUpdateBody {
        const bool IsSnapshot;
        const ui32 Step;
        TString EmbeddedBody;
        TVector<std::pair<TLogoBlobID, TString>> References;
        TString AuxPayload;

        bool NeedFollowerGcAck;

        TFUpdateBody(const TEvFollowerUpdate &upd)
            : IsSnapshot(upd.Record.GetIsSnapshot())
            , Step(upd.Record.GetStep())
            , NeedFollowerGcAck(upd.Record.HasNeedGCApplyAck() ? upd.Record.GetNeedGCApplyAck() : false)
        {
            const auto &r = upd.Record;
            if (r.HasBody())
                EmbeddedBody = r.GetBody();
            if (r.HasAuxPayload())
                AuxPayload = r.GetAuxPayload();

            References.reserve(r.ReferencesIdsSize());
            for (ui32 i = 0, end = r.ReferencesIdsSize(); i != end; ++i) {
                const TLogoBlobID &id = LogoBlobIDFromLogoBlobID(r.GetReferencesIds(i));
                References.emplace_back(std::make_pair(id, r.GetReferences(i)));
            }
        }

        TFUpdateBody(TString auxUpdate)
            : IsSnapshot(false)
            , Step(0)
            , AuxPayload(std::move(auxUpdate))
            , NeedFollowerGcAck(false)
        {}
    };

    struct TEvFBoot : public TEventLocal<TEvFBoot, EvFBoot> {
        // boot new round of follower
        const ui64 TabletID;
        const ui32 FollowerID;
        const ui32 Generation;

        const TActorId Launcher;

        // must be present one of: or loaded graph or snapshot follower update
        TIntrusivePtr<TDependencyGraph> DependencyGraph;
        THolder<TFUpdateBody> Update;

        TIntrusivePtr<TTabletStorageInfo> TabletStorageInfo;

        TResourceProfilesPtr ResourceProfiles;
        TSharedQuotaPtr TxCacheQuota;

        NMetrics::TTabletThroughputRawValue GroupReadBytes;
        NMetrics::TTabletIopsRawValue GroupReadOps;

        TEvFBoot(ui64 tabletID, ui32 followerID, ui32 generation, TActorId launcher, const TEvFollowerUpdate &upd,
                 TIntrusivePtr<TTabletStorageInfo> info, TResourceProfilesPtr profiles = nullptr,
                 TSharedQuotaPtr txCacheQuota = nullptr)
            : TabletID(tabletID)
            , FollowerID(followerID)
            , Generation(generation)
            , Launcher(launcher)
            , Update(new TFUpdateBody(upd))
            , TabletStorageInfo(info)
            , ResourceProfiles(profiles)
            , TxCacheQuota(txCacheQuota)
        {}

        TEvFBoot(ui64 tabletID, ui32 followerID, ui32 generation, TActorId launcher, TDependencyGraph *dependencyGraph,
            TIntrusivePtr<TTabletStorageInfo> info, TResourceProfilesPtr profiles = nullptr,
            TSharedQuotaPtr txCacheQuota = nullptr,
            NMetrics::TTabletThroughputRawValue&& read = NMetrics::TTabletThroughputRawValue(),
            NMetrics::TTabletIopsRawValue&& readOps = NMetrics::TTabletIopsRawValue())
            : TabletID(tabletID)
            , FollowerID(followerID)
            , Generation(generation)
            , Launcher(launcher)
            , DependencyGraph(dependencyGraph)
            , TabletStorageInfo(info)
            , ResourceProfiles(profiles)
            , TxCacheQuota(txCacheQuota)
            , GroupReadBytes(std::move(read))
            , GroupReadOps(std::move(readOps))
        {}
    };

    struct TEvFUpdate : public TEventLocal<TEvFUpdate, EvFUpdate> {
        THolder<TFUpdateBody> Update;

        TEvFUpdate(const TEvFollowerUpdate &upd)
            : Update(new TFUpdateBody(upd))
        {}
    };

    struct TEvFAuxUpdate : public TEventLocal<TEvFAuxUpdate, EvFAuxUpdate> {
        const TString AuxUpdate;

        TEvFAuxUpdate(const TString &auxUpdate)
            : AuxUpdate(auxUpdate)
        {}
    };

    struct TEvFollowerGcApplied : public TEventLocal<TEvFollowerGcApplied, EvFollowerGcApplied> {
        const ui64 TabletID;
        const ui32 Generation;
        const ui32 Step;
        const TDuration FollowerSyncDelay;

        TEvFollowerGcApplied(ui64 tabletId, ui32 gen, ui32 step, TDuration followerSyncDelay)
            : TabletID(tabletId)
            , Generation(gen)
            , Step(step)
            , FollowerSyncDelay(followerSyncDelay)
        {}
    };

    struct TEvFollowerSyncComplete : public TEventLocal<TEvFollowerSyncComplete, EvFollowerSyncComplete> {};

    struct TEvFGcAck : public TEventLocal<TEvFGcAck, EvFGcAck> {
        const ui64 TabletID;
        const ui32 Generation;
        const ui32 Step;

        TEvFGcAck(ui64 tabletId, ui32 gen, ui32 step)
            : TabletID(tabletId)
            , Generation(gen)
            , Step(step)
        {}
    };

    struct TEvCheckBlobstorageStatusResult : public TEventLocal<TEvCheckBlobstorageStatusResult, EvCheckBlobstorageStatusResult> {
        TVector<ui32> LightYellowMoveGroups;
        TVector<ui32> YellowStopGroups;

        TEvCheckBlobstorageStatusResult(TVector<ui32> &&lightYellowMoveGroups, TVector<ui32> &&yellowStopGroups)
            : LightYellowMoveGroups(std::move(lightYellowMoveGroups))
            , YellowStopGroups(std::move(yellowStopGroups))

        {}
    };

    struct TEvResetTabletResult : public TEventLocal<TEvResetTabletResult, EvResetTabletResult> {
        const NKikimrProto::EReplyStatus Status;
        const ui64 TabletId;

        TEvResetTabletResult(NKikimrProto::EReplyStatus status, ui64 tabletId)
            : Status(status)
            , TabletId(tabletId)
        {}
    };

    struct TEvGetCounters : TEventPB<TEvGetCounters, NKikimrTabletBase::TEvGetCounters, EvGetCounters> {};
    struct TEvGetCountersResponse : TEventPB<TEvGetCountersResponse, NKikimrTabletBase::TEvGetCountersResponse, EvGetCountersResponse> {};

    struct TEvCutTabletHistory : TEventPB<TEvCutTabletHistory, NKikimrTabletBase::TEvCutTabletHistory, EvCutTabletHistory> {
        TEvCutTabletHistory() = default;
    };

    struct TEvUpdateConfig : TEventLocal<TEvUpdateConfig, EvUpdateConfig> {
        TResourceProfilesPtr ResourceProfiles;

        TEvUpdateConfig(TResourceProfilesPtr profiles)
            : ResourceProfiles(profiles)
        {}
    };

    struct TEvFeatures : TEventLocal<TEvFeatures, EvFeatures> {
        enum EFeatureFlags : ui32 {
            None = 0,
            GracefulStop = 1,
        };

        const ui32 Features;

        explicit TEvFeatures(ui32 features)
            : Features(features)
        { }
    };

    struct TEvTabletStop : TEventPB<TEvTabletStop, NKikimrTabletBase::TEvTabletStop, EvTabletStop> {
        using EReason = NKikimrTabletBase::TEvTabletStop::EReason;

#define DEFINE_TABLET_STOP_REASON(name) static constexpr EReason name = NKikimrTabletBase::TEvTabletStop::name
        DEFINE_TABLET_STOP_REASON(ReasonUnknown);
        DEFINE_TABLET_STOP_REASON(ReasonStop);
        DEFINE_TABLET_STOP_REASON(ReasonDemoted);
        DEFINE_TABLET_STOP_REASON(ReasonIsolated);
        DEFINE_TABLET_STOP_REASON(ReasonStorageBlocked);
        DEFINE_TABLET_STOP_REASON(ReasonStorageFailure);
#undef DEFINE_TABLET_STOP_REASON

        TEvTabletStop() = default;

        explicit TEvTabletStop(ui64 tabletId, EReason reason) {
            Record.SetTabletID(tabletId);
            Record.SetReason(reason);
        }

        ui64 GetTabletID() const { return Record.GetTabletID(); }
        EReason GetReason() const { return Record.GetReason(); }
    };

    struct TEvTabletStopped : TEventLocal<TEvTabletStopped, EvTabletStopped> {};

    struct TEvDropLease : TEventPB<TEvDropLease, NKikimrTabletBase::TEvDropLease, EvDropLease> {
        TEvDropLease() = default;

        explicit TEvDropLease(ui64 tabletId) {
            Record.SetTabletID(tabletId);
        }
    };

    struct TEvLeaseDropped : TEventPB<TEvLeaseDropped, NKikimrTabletBase::TEvLeaseDropped, EvLeaseDropped> {
        TEvLeaseDropped() = default;

        explicit TEvLeaseDropped(ui64 tabletId) {
            Record.SetTabletID(tabletId);
        }
    };
};

IActor* CreateTabletKiller(ui64 tabletId, ui32 nodeId = 0, ui32 maxGeneration = Max<ui32>());
IActor* CreateTabletDSChecker(const TActorId &replyTo, TTabletStorageInfo *info);
IActor* CreateTabletReqReset(const TActorId &replyTo, const TIntrusivePtr<TTabletStorageInfo> &tabletStorageInfo, ui32 knownGeneration = 0);

}

template<>
inline void Out<NKikimr::TEvTablet::TEvTabletDead::EReason>(IOutputStream& o, NKikimr::TEvTablet::TEvTabletDead::EReason x) {
    return NKikimr::TEvTablet::TEvTabletDead::Out(o, x);
}

inline TString ToString(NKikimr::TEvTablet::TEvTabletDead::EReason x) {
    return NKikimr::TEvTablet::TEvTabletDead::Str(x);
}
