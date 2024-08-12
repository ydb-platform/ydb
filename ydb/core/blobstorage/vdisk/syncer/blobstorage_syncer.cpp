#include "blobstorage_syncer.h"
#include "blobstorage_syncer_scheduler.h"
#include "blobstorage_syncer_localwriter.h"
#include "syncer_job_task.h"
#include "blobstorage_syncer_committer.h"
#include "blobstorage_syncer_recoverlostdata.h"
#include "guid_recovery.h"
#include "guid_propagator.h"
#include <ydb/core/blobstorage/base/html.h>
#include <ydb/core/blobstorage/vdisk/common/blobstorage_dblogcutter.h>
#include <ydb/core/blobstorage/vdisk/common/blobstorage_status.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_public_events.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <ydb/library/actors/core/mon.h>

using namespace NKikimrServices;
using namespace NKikimr::NSyncer;

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TDelayedQueue
    ////////////////////////////////////////////////////////////////////////////
    class TDelayedQueue {
    public:
        using TItem = TEvBlobStorage::TEvVSyncGuidResult;

    public:
        ui64 WriteRequest(const TActorId &id, std::unique_ptr<TItem> &&r) {
            ui64 seqNum = ++InFlySeqNum;
            Queue.emplace_back(id, std::move(r), seqNum);
            return seqNum;
        }

        void ReadRequest(const TActorContext &ctx, const TActorId &id, std::unique_ptr<TItem> &&r) {
            if (CommittedSeqNum == InFlySeqNum) {
                ctx.Send(id, r.release());
            } else {
                Queue.emplace_back(id, std::move(r), InFlySeqNum);
            }
        }

        void WriteReady(const TActorContext &ctx, ui64 seqNum) {
            Y_ABORT_UNLESS(!Queue.empty());
            CommittedSeqNum = seqNum;

            while (!Queue.empty() && Queue.front().SeqNum <= CommittedSeqNum) {
                auto &elem = Queue.front();
                ctx.Send(elem.ActorId, elem.Item.release());
                Queue.pop_front();
            }
        }

    private:
        struct TItemAndSeq {
            TActorId ActorId;
            std::unique_ptr<TItem> Item;
            ui64 SeqNum;

            TItemAndSeq(const TActorId &id, std::unique_ptr<TItem> &&r, ui64 seqNum)
                : ActorId(id)
                , Item(std::move(r))
                , SeqNum(seqNum)
            {}
        };

        using TQueueType = TDeque<TItemAndSeq>;

        TQueueType Queue;
        ui64 InFlySeqNum = 0;
        ui64 CommittedSeqNum = 0;
    };

    ////////////////////////////////////////////////////////////////////////////
    // TSyncerHttpInfoActor
    ////////////////////////////////////////////////////////////////////////////
    class TSyncerHttpInfoActor : public TActorBootstrapped<TSyncerHttpInfoActor> {
        TIntrusivePtr<TSyncerContext> SyncerCtx;
        NMon::TEvHttpInfo::TPtr Ev;
        const TActorId ReplyId;
        const TActorId NotifyId;
        const TActorId SchedulerId;
        const TString LogAndPhase;

        friend class TActorBootstrapped<TSyncerHttpInfoActor>;

        void Bootstrap(const TActorContext &ctx) {
            if (SchedulerId == TActorId()) {
                RenderHtmlAndReply(ctx, TString());
            } else {
                Become(&TThis::StateFunc);
                ctx.Send(SchedulerId, Ev->Release().Release());
            }
        }

        void RenderHtmlAndReply(const TActorContext &ctx, const TString &schedulerInfo) {
            TStringStream str;
            str << "\n";
            HTML(str) {
                DIV_CLASS("panel panel-warning") {
                    DIV_CLASS("panel-heading") {str << "Syncer";}
                    DIV_CLASS("panel-body") {
                        str << LogAndPhase;
                        str << schedulerInfo;
                    }
                }
            }
            str << "\n";

            ctx.Send(ReplyId, new NMon::TEvHttpInfoRes(str.Str(), TDbMon::SyncerInfoId));
            ctx.Send(NotifyId, new TEvents::TEvActorDied());
            Die(ctx);
        }

        void Handle(NMon::TEvHttpInfoRes::TPtr &ev, const TActorContext &ctx) {
            TStringStream str;
            ev->Get()->Output(str);
            RenderHtmlAndReply(ctx, str.Str());
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
                      HFunc(NMon::TEvHttpInfoRes, Handle)
                      HFunc(TEvents::TEvPoisonPill, HandlePoison)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_SYNCER_HTTPREQ;
        }

        TSyncerHttpInfoActor(TIntrusivePtr<TSyncerContext> &sc,
                             NMon::TEvHttpInfo::TPtr &ev,
                             const TActorId &notifyId,
                             const TActorId &schedulerId,
                             const TString &logAndPhase)
            : TActorBootstrapped<TSyncerHttpInfoActor>()
            , SyncerCtx(sc)
            , Ev(ev)
            , ReplyId(Ev->Sender)
            , NotifyId(notifyId)
            , SchedulerId(schedulerId)
            , LogAndPhase(logAndPhase)
        {
            Y_ABORT_UNLESS(Ev->Get()->SubRequestId == TDbMon::SyncerInfoId);
        }
    };


    ////////////////////////////////////////////////////////////////////////////
    // TSyncer
    ////////////////////////////////////////////////////////////////////////////
    class TSyncer : public TActorBootstrapped<TSyncer> {
        // from protobuf
        using ESyncStatus = NKikimrVDiskData::TSyncerVDiskEntry::ESyncStatus;
        using TSyncStatusVal = NKikimrVDiskData::TSyncerVDiskEntry;
        using EPhase = NKikimrBlobStorage::TSyncerStatus::EPhase;
        using TPhaseVal = NKikimrBlobStorage::TSyncerStatus;
        // other
        using TVDiskInfoPtr = const TSyncNeighbors::TValue *;

        TIntrusivePtr<TSyncerContext> SyncerCtx;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        TIntrusivePtr<TSyncerData> SyncerData;
        TLocalSyncerState LocalSyncerState;
        TActorId CommitterId;
        TActorId SchedulerId;
        TActorId GuidRecoveryId;
        TActorId RecoverLostDataId;
        TVector<TActorId> PropagatorIds;
        EPhase Phase = TPhaseVal::PhaseNone;
        TActiveActors ActiveActors;
        std::unique_ptr<NSyncer::TOutcome> GuidRecovOutcome;
        TDelayedQueue DelayedQueue;
        TSublog<> Sublog;

        friend class TActorBootstrapped<TSyncer>;

        void Bootstrap(const TActorContext &ctx) {
            // create committer with current state
            TSyncerDataSerializer sds;
            SyncerData->Serialize(sds, GInfo.Get());
            CommitterId = ctx.Register(CreateSyncerCommitter(SyncerCtx, std::move(sds)));
            ActiveActors.Insert(CommitterId, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
            // Sync VDisk Guid
            SyncGuid(ctx);
        }

        ////////////////////////////////////////////////////////////////////////
        // Run guid propogators
        ////////////////////////////////////////////////////////////////////////
        void RunPropagators(const TActorContext &ctx) {
            auto state = NKikimrBlobStorage::TSyncGuidInfo::Final;
            auto guid = GuidRecovOutcome->Guid;
            NSync::TVDiskNeighbors<int> neighbors(SyncerCtx->VCtx->ShortSelfVDisk, SyncerCtx->VCtx->Top);
            auto selfVDiskId = GInfo->GetVDiskId(SyncerCtx->VCtx->ShortSelfVDisk);
            PropagatorIds.reserve(SyncerCtx->VCtx->Top->GetTotalVDisksNum());
            for (auto &x : neighbors) {
                const TVDiskID vd = GInfo->GetVDiskId(x.OrderNumber);
                const TActorId va = GInfo->GetActorId(x.OrderNumber);
                auto aid = ctx.Register(CreateSyncerGuidPropagator(SyncerCtx->VCtx,
                                                                   selfVDiskId,
                                                                   vd,
                                                                   va,
                                                                   state,
                                                                   guid));
                ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
                PropagatorIds.push_back(aid);
            }
        }

        ////////////////////////////////////////////////////////////////////////
        // Sync Guid
        ////////////////////////////////////////////////////////////////////////
        void SyncGuid(const TActorContext &ctx) {
            Become(&TThis::SyncGuidStateFunc);
            GuidRecoveryId = ctx.Register(CreateVDiskGuidRecoveryActor(SyncerCtx->VCtx, GInfo, CommitterId, SelfId(),
                LocalSyncerState, SyncerCtx->Config->BaseInfo.ReadOnly));
            ActiveActors.Insert(GuidRecoveryId, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
            Phase = TPhaseVal::PhaseSyncGuid;
        }

        void Handle(TEvVDiskGuidRecovered::TPtr &ev, const TActorContext &ctx) {
            ActiveActors.Erase(ev->Sender);
            GuidRecoveryId = TActorId();
            GuidRecovOutcome = std::make_unique<NSyncer::TOutcome>(std::move(ev->Get()->Outcome));

            switch (GuidRecovOutcome->Decision) {
                case EDecision::FirstRun:
                case EDecision::Good:
                    StandardMode(ctx);
                    LOG_DEBUG(ctx, NKikimrServices::BS_VDISK_CHUNKS, VDISKP(SyncerCtx->VCtx->VDiskLogPrefix, "GUID: PDiskId# %s Good", SyncerCtx->PDiskCtx->PDiskIdString.data()));
                    break;
                case EDecision::LostData:
                    LOG_DEBUG(ctx, NKikimrServices::BS_VDISK_CHUNKS, VDISKP(SyncerCtx->VCtx->VDiskLogPrefix, "GUID: PDiskId# %s LostData", SyncerCtx->PDiskCtx->PDiskIdString.data()));
                    RecoverLostData(ctx);
                    break;
                case EDecision::Inconsistency:
                    LOG_DEBUG(ctx, NKikimrServices::BS_VDISK_CHUNKS, VDISKP(SyncerCtx->VCtx->VDiskLogPrefix, "GUID: PDiskId# %s Inconsistency", SyncerCtx->PDiskCtx->PDiskIdString.data()));
                    InconsistentState(ctx);
                    break;
                default:
                    Y_ABORT("Unexpected case");
            }
        }

        STRICT_STFUNC(SyncGuidStateFunc,
            HFunc(TEvBlobStorage::TEvVSyncGuid, Handle)
            HFunc(TEvSyncerCommitDone, Handle)
            HFunc(TEvVDiskGuidRecovered, Handle)
            HFunc(NMon::TEvHttpInfo, Handle)
            HFunc(TEvLocalStatus, Handle)
            HFunc(NPDisk::TEvCutLog, Handle)
            HFunc(TEvents::TEvActorDied, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            HFunc(TEvSublogLine, Handle)
            HFunc(TEvVGenerationChange, SyncGuidModeHandle)
        )

        ////////////////////////////////////////////////////////////////////////
        // Inconsistent State
        ////////////////////////////////////////////////////////////////////////
        void InconsistentState(const TActorContext &ctx) {
            Become(&TThis::InconsistentancyStateFunc);
            Phase = TPhaseVal::PhaseInconsistency;
            // notify Skeleton about SyncGuid recovery state
            ctx.Send(SyncerData->NotifyId, new TEvSyncGuidRecoveryDone(NKikimrProto::ERROR, 0));
        }

        STRICT_STFUNC(InconsistentancyStateFunc,
            HFunc(TEvBlobStorage::TEvVSyncGuid, Handle)
            HFunc(TEvSyncerCommitDone, Handle)
            HFunc(NMon::TEvHttpInfo, Handle)
            HFunc(TEvLocalStatus, Handle)
            HFunc(NPDisk::TEvCutLog, Handle)
            HFunc(TEvents::TEvActorDied, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            HFunc(TEvSublogLine, Handle)
            HFunc(TEvVGenerationChange, InconsistencyModeHandle)
        )

        ////////////////////////////////////////////////////////////////////////
        // Recover Lost Data
        ////////////////////////////////////////////////////////////////////////
        void RecoverLostData(const TActorContext &ctx) {
            if (SyncerCtx->Config->BaseInfo.ReadOnly) {
                LOG_WARN(ctx, BS_SYNCER,
                    VDISKP(SyncerCtx->VCtx->VDiskLogPrefix,
                        "Unable to recover lost data in read-only mode. Transitioning to inconsistent state."));
                InconsistentState(ctx);
                return;
            }

            Become(&TThis::RecoverLostDataStateFunc);
            Phase = TPhaseVal::PhaseRecoverLostData;

            if (SyncerCtx->Config->EnableVDiskCooldownTimeout) {
                Schedule(SyncerCtx->Config->BaseInfo.YardInitDelay, new TEvents::TEvWakeup);
            } else {
                RecoverLostDataResumeAfterDelay(ctx);
            }
        }

        void RecoverLostDataResumeAfterDelay(const TActorContext& ctx) {
            const TVDiskEternalGuid guid = GuidRecovOutcome->Guid;
            RecoverLostDataId = ctx.Register(CreateSyncerRecoverLostDataActor(SyncerCtx, GInfo, CommitterId, ctx.SelfID, guid));
            ActiveActors.Insert(RecoverLostDataId, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
        }

        void Handle(TEvSyncerLostDataRecovered::TPtr &ev, const TActorContext &ctx) {
            ActiveActors.Erase(ev->Sender);
            RecoverLostDataId = TActorId();
            LocalSyncerState = ev->Get()->LocalSyncerState;
            StandardMode(ctx);
        }

        STRICT_STFUNC(RecoverLostDataStateFunc,
            HFunc(TEvBlobStorage::TEvVSyncGuid, Handle)
            HFunc(TEvSyncerCommitDone, Handle)
            HFunc(TEvSyncerLostDataRecovered, Handle)
            HFunc(NMon::TEvHttpInfo, Handle)
            HFunc(TEvLocalStatus, Handle)
            HFunc(NPDisk::TEvCutLog, Handle)
            HFunc(TEvents::TEvActorDied, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            HFunc(TEvSublogLine, Handle)
            HFunc(TEvVGenerationChange, RecoverLostDataModeHandle)
            CFunc(TEvents::TSystem::Wakeup, RecoverLostDataResumeAfterDelay);
        )

        ////////////////////////////////////////////////////////////////////////
        // Standard Mode
        ////////////////////////////////////////////////////////////////////////
        void StandardMode(const TActorContext &ctx) {
            // notify synclog that it can start serving requests, tell him the DbBirthLsn
            auto msg = std::make_unique<NSyncLog::TEvSyncLogDbBirthLsn>(LocalSyncerState.DbBirthLsn);
            ctx.Send(SyncerCtx->SyncLogId, msg.release());

            RunPropagators(ctx);
            // notify Skeleton about SyncGuid recovery state
            ctx.Send(SyncerData->NotifyId,
                     new TEvSyncGuidRecoveryDone(NKikimrProto::OK, LocalSyncerState.DbBirthLsn));
            SyncerData->Neighbors->DbBirthLsn = LocalSyncerState.DbBirthLsn;
            Become(&TThis::StandardModeStateFunc);
            if (!SyncerCtx->Config->BaseInfo.ReadOnly) {
                LOG_DEBUG(ctx, BS_SYNCER,
                    VDISKP(SyncerCtx->VCtx->VDiskLogPrefix,
                        "%s: Creating syncer scheduler on node %d", __PRETTY_FUNCTION__, SelfId().NodeId()));
                SchedulerId = ctx.Register(CreateSyncerSchedulerActor(SyncerCtx, GInfo, SyncerData, CommitterId));
                ActiveActors.Insert(SchedulerId, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
            } else {
                LOG_WARN(ctx, BS_SYNCER,
                    VDISKP(SyncerCtx->VCtx->VDiskLogPrefix,
                        "%s: Skipping scheduler start due to read-only", __PRETTY_FUNCTION__));
            }
            Phase = TPhaseVal::PhaseStandardMode;
        }

        STRICT_STFUNC(StandardModeStateFunc,
            HFunc(TEvBlobStorage::TEvVSyncGuid, Handle)
            HFunc(TEvSyncerCommitDone, Handle)
            HFunc(NMon::TEvHttpInfo, Handle)
            HFunc(TEvLocalStatus, Handle)
            HFunc(NPDisk::TEvCutLog, Handle)
            HFunc(TEvents::TEvActorDied, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            HFunc(TEvSublogLine, Handle)
            HFunc(TEvVGenerationChange, ReadyModeHandle)
        )

        ////////////////////////////////////////////////////////////////////////
        // Handle EvVSyncGuid
        ////////////////////////////////////////////////////////////////////////
        void Handle(TEvBlobStorage::TEvVSyncGuid::TPtr &ev, const TActorContext &ctx) {
            const NKikimrBlobStorage::TEvVSyncGuid &record = ev->Get()->Record;
            TVDiskID vdisk = VDiskIDFromVDiskID(record.GetSourceVDiskID());
            auto selfVDisk = GInfo->GetVDiskId(SyncerCtx->VCtx->ShortSelfVDisk);
            if (record.HasInfo()) {
                // handle WRITE request
                auto &info = record.GetInfo();
                auto state = info.GetState();
                auto guid = info.GetGuid();
                // update local
                // FIXME: check that this CACHE works correctly. It can be a good
                // idea to forward all this messages directly to Committer, because
                // is knows exact values
                const ui64 prevGuidInMemory = (*SyncerData->Neighbors)[vdisk].Get().PeerGuidInfo.Info.GetGuid();
                auto &data = (*SyncerData->Neighbors)[vdisk].Get().PeerGuidInfo;
                data.Info = info;
                // create reply
                auto result = std::make_unique<TEvBlobStorage::TEvVSyncGuidResult>(NKikimrProto::OK, selfVDisk,
                    TAppData::TimeProvider->Now(), nullptr, nullptr, ev->GetChannel());
                if (!SyncerCtx->Config->BaseInfo.ReadOnly) {
                    // put reply into the queue and wait until it would be committed
                    ui64 seqNum = DelayedQueue.WriteRequest(ev->Sender, std::move(result));
                    // commit
                    void *cookie = reinterpret_cast<void*>(intptr_t(seqNum));
                    auto msg = TEvSyncerCommit::Remote(vdisk, state, guid, cookie);
                    ctx.Send(CommitterId, msg.release());
                } else {
                    LOG_WARN(ctx, BS_SYNCER,
                        VDISKP(SyncerCtx->VCtx->VDiskLogPrefix,
                            "%s: Skipping commit of incoming EvVSyncGuid: saved guid %s",
                            __PRETTY_FUNCTION__, prevGuidInMemory != guid ? "differs" : "matches"));
                }
            } else {
                // handle READ request
                auto &data = (*SyncerData->Neighbors)[vdisk].Get().PeerGuidInfo.Info;
                auto state = data.GetState();
                auto guid = data.GetGuid();
                // create reply
                auto result = std::make_unique<TEvBlobStorage::TEvVSyncGuidResult>(NKikimrProto::OK, selfVDisk,
                    TAppData::TimeProvider->Now(), guid, state, nullptr, nullptr, ev->GetChannel());
                // put reply into the queue and wait until all required writes are committed
                DelayedQueue.ReadRequest(ctx, ev->Sender, std::move(result));
            }
        }

        void Handle(TEvSyncerCommitDone::TPtr &ev, const TActorContext &ctx) {
            const void *cookie = ev->Get()->Cookie;
            ui64 seqNum = reinterpret_cast<ui64>(cookie);
            DelayedQueue.WriteReady(ctx, seqNum);
        }


        ////////////////////////////////////////////////////////////////////////
        // Handle TEvHttpInfo
        ////////////////////////////////////////////////////////////////////////
        static NKikimrWhiteboard::EFlag ToSignalLight(EPhase phase) {
            switch (phase) {
                case TPhaseVal::PhaseNone:              return NKikimrWhiteboard::EFlag::Red;
                case TPhaseVal::PhaseSyncGuid:          return NKikimrWhiteboard::EFlag::Yellow;
                case TPhaseVal::PhaseRecoverLostData:   return NKikimrWhiteboard::EFlag::Yellow;
                case TPhaseVal::PhaseStandardMode:      return NKikimrWhiteboard::EFlag::Green;
                case TPhaseVal::PhaseInconsistency:     return NKikimrWhiteboard::EFlag::Red;
                default:                                return NKikimrWhiteboard::EFlag::Red;
            }
        }

        static const char *ToString(EPhase phase) {
            switch (phase) {
                case TPhaseVal::PhaseNone:              return "None";
                case TPhaseVal::PhaseSyncGuid:          return "SyncGuid";
                case TPhaseVal::PhaseRecoverLostData:   return "RecoverLostData";
                case TPhaseVal::PhaseStandardMode:      return "StandardMode";
                case TPhaseVal::PhaseInconsistency:     return "Inconsistency";
                default:                                return "UNKNOWN";
            }
        }

        void LogAndPhaseToHtml(IOutputStream &str) const {
            HTML(str) {
                DIV_CLASS("row") {
                    str << "Phase: ";
                    THtmlLightSignalRenderer(ToSignalLight(Phase), ToString(Phase)).Output(str);
                }
                COLLAPSED_BUTTON_CONTENT("syncerlogid", "Log") {
                    PRE() {str << Sublog.Get();}
                }
            }
        }

        void Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {
            Y_ABORT_UNLESS(ev->Get()->SubRequestId == TDbMon::SyncerInfoId);
            TActorId schId;
            switch (Phase) {
                case TPhaseVal::PhaseStandardMode: {
                    schId = SchedulerId;
                    break;
                }
                case TPhaseVal::PhaseInconsistency:
                case TPhaseVal::PhaseSyncGuid:
                case TPhaseVal::PhaseRecoverLostData:
                    // no SchedulerId
                    break;
                default: Y_ABORT("Unexpected case");
            }

            // print out local info
            TStringStream str;
            LogAndPhaseToHtml(str);
            // create an actor to handle request
            auto actor = std::make_unique<TSyncerHttpInfoActor>(SyncerCtx, ev, ctx.SelfID, schId, str.Str());
            auto aid = ctx.Register(actor.release());
            ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
        }

        void Handle(TEvSublogLine::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ctx);
            Sublog.Log() << ev->Get()->GetLine() << "\n";
        }

        ////////////////////////////////////////////////////////////////////////
        // Other handlers
        ////////////////////////////////////////////////////////////////////////
        void Handle(TEvLocalStatus::TPtr &ev, const TActorContext &ctx) {
            if (Phase == TPhaseVal::PhaseStandardMode && SchedulerId) {
                ctx.Send(ev->Forward(SchedulerId));
            } else {
                auto result = std::make_unique<TEvLocalStatusResult>();
                NKikimrBlobStorage::TSyncerStatus *rec = result->Record.MutableSyncerStatus();
                rec->SetPhase(Phase);
                ctx.Send(ev->Sender, result.release());
            }
        }

        void Handle(NPDisk::TEvCutLog::TPtr &ev, const TActorContext &ctx) {
            ctx.Send(ev->Forward(CommitterId));
        }

        // This handler is called when TSyncerHttpInfoActor is finished
        void Handle(TEvents::TEvActorDied::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ctx);
            ActiveActors.Erase(ev->Sender);
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            ActiveActors.KillAndClear(ctx);
            Die(ctx);
        }

        ////////////////////////////////////////////////////////////////////////
        // BlobStorage Group Reconfiguration
        ////////////////////////////////////////////////////////////////////////
        void ReadyModeHandle(TEvVGenerationChange::TPtr &ev, const TActorContext &ctx) {
            auto *msg = ev->Get();
            Sublog.Log() << "Syncer: GenerationChange (ReadyModeHandle)\n";
            // check that NewInfo has the same topology as the one VDisk started with
            Y_ABORT_UNLESS(SyncerCtx->VCtx->Top->EqualityCheck(msg->NewInfo->GetTopology()));

            GInfo = msg->NewInfo;
            // reconfigure scheduler
            if (SchedulerId) {
                ctx.Send(SchedulerId, msg->Clone());
            }
            // reconfigure propagators
            for (const auto &aid : PropagatorIds) {
                ctx.Send(aid, msg->Clone());
            }
        }

        void SyncGuidModeHandle(TEvVGenerationChange::TPtr &ev, const TActorContext &ctx) {
            auto *msg = ev->Get();
            Sublog.Log() << "Syncer: GenerationChange (SyncGuidModeHandle)\n";
            // check that NewInfo has the same topology as the one VDisk started with
            Y_ABORT_UNLESS(SyncerCtx->VCtx->Top->EqualityCheck(msg->NewInfo->GetTopology()));

            GInfo = msg->NewInfo;
            // reconfigure guid recovery actor
            Y_ABORT_UNLESS(GuidRecoveryId != TActorId());
            ctx.Send(GuidRecoveryId, msg->Clone());
        }

        void InconsistencyModeHandle(TEvVGenerationChange::TPtr &ev, const TActorContext &ctx) {
            auto *msg = ev->Get();
            Sublog.Log() << "Syncer: GenerationChange (InconsistencyModeHandle)\n";
            // check that NewInfo has the same topology as the one VDisk started with
            Y_ABORT_UNLESS(SyncerCtx->VCtx->Top->EqualityCheck(msg->NewInfo->GetTopology()));

            Y_UNUSED(ctx);
            GInfo = msg->NewInfo;
        }

        void RecoverLostDataModeHandle(TEvVGenerationChange::TPtr &ev, const TActorContext &ctx) {
            auto *msg = ev->Get();
            Sublog.Log() << "Syncer: GenerationChange (RecoverLostDataModeHandle)\n";
            // check that NewInfo has the same topology as the one VDisk started with
            Y_ABORT_UNLESS(SyncerCtx->VCtx->Top->EqualityCheck(msg->NewInfo->GetTopology()));

            GInfo = msg->NewInfo;

            // reconfigure guid recovery actor
            Y_ABORT_UNLESS(RecoverLostDataId != TActorId());
            ctx.Send(RecoverLostDataId, msg->Clone());
        }

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_SYNCER_MAIN;
        }

        TSyncer(const TIntrusivePtr<TSyncerContext> &sc,
                const TIntrusivePtr<TBlobStorageGroupInfo> &info,
                const TIntrusivePtr<TSyncerData> &syncerData)
            : TActorBootstrapped<TSyncer>()
            , SyncerCtx(sc)
            , GInfo(info)
            , SyncerData(syncerData)
            , LocalSyncerState(SyncerData->LocalSyncerState)
        {
            Y_ABORT_UNLESS(SyncerCtx->VCtx->Top->EqualityCheck(info->GetTopology()));
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // SYNCER ACTOR CREATOR
    ////////////////////////////////////////////////////////////////////////////
    IActor* CreateSyncerActor(const TIntrusivePtr<TSyncerContext> &sc,
                              const TIntrusivePtr<TBlobStorageGroupInfo> &info,
                              const TIntrusivePtr<TSyncerData> &syncerData) {
        return new TSyncer(sc, info, syncerData);
    }
} // NKikimr
