#include "syncer_job_task.h"
#include "syncer_context.h"
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogmsgreader.h>

using namespace NKikimrServices;

namespace NKikimr {
    namespace NSyncer {

        ////////////////////////////////////////////////////////////////////////////
        // TSjCtx - Syncer Job Context
        ////////////////////////////////////////////////////////////////////////////
        TSjCtx::TSjCtx(const TVDiskID &self, const TIntrusivePtr<TSyncerContext> &sc)
            : SelfVDiskId(self)
            , SyncerCtx(sc)
        {}

        // create TSjCtx using actual Db and GInfo
        std::shared_ptr<TSjCtx> TSjCtx::Create(const TIntrusivePtr<TSyncerContext> &sc,
                                                const TIntrusivePtr<TBlobStorageGroupInfo> &info)
        {
            const TVDiskID vdiskId = info->GetVDiskId(sc->VCtx->ShortSelfVDisk);
            // to prevent memory leaks -- unique_ptr ctor does not throw, unlike shared_ptr's one
            return std::shared_ptr<TSjCtx>(std::unique_ptr<TSjCtx>(new TSjCtx(vdiskId, sc)));
        }


        ////////////////////////////////////////////////////////////////////////////
        // TSyncerJobTask
        ////////////////////////////////////////////////////////////////////////////
        const char *TSyncerJobTask::EJobTypeToStr(EJobType t) {
            switch (t) {
                case EFullRecover:  return "FullRecover";
                case EJustSync:     return "JustSync";
                default:            return "UNKNOWN";
            }
        }

        const char *TSyncerJobTask::EPhaseToStr(EPhase t) {
            switch (t) {
                case EStart:        return "Start";
                case EWaitRemote:   return "WaitRemote";
                case EWaitLocal:    return "WaitLocal";
                case EFinished:     return "Finished";
                case ETerminated:   return "Terminated";
                default:            return "UNKNOWN";
            }
        }

        TSyncerJobTask::TSyncerJobTask(
                         EJobType type,
                         const TVDiskID &vdisk,
                         const TActorId &service,
                         const NSyncer::TPeerSyncState &peerState,
                         const std::shared_ptr<TSjCtx> &ctx)
            : VDiskId(vdisk)
            , ServiceId(service)
            , Type(type)
            , OldSyncState(peerState.SyncState)
            , Current(peerState)
            , Ctx(ctx)
            , Sublog(false, Ctx->SelfVDiskId.ToString() + ": ")
        {
            Current.LastSyncStatus = TSyncStatusVal::Running;
            if (Type == EFullRecover) {
                PrepareToFullRecovery(TSyncState());
            }
            Sublog.Log() << "TSyncerJobTask::TSyncerJobTask: Type# " << EJobTypeToStr(Type)
                << " target# " << VDiskId.ToString() << "\n";
        }

        void TSyncerJobTask::Output(IOutputStream &str) const {
            str << "{SjTask: targetVDisk# " << VDiskId.ToString()
                << " Type# " << EJobTypeToStr(Type)
                << " Phase# " << EPhaseToStr(Phase)
                << " Current# " << Current.ToString()
                << " SentMsg# " << SentMsg
                << " BytesReceived# " << BytesReceived
                << " RedirCounter# " << RedirCounter
                << " EndOfStream# " << EndOfStream
                << "}";
        }

        TString TSyncerJobTask::ToString() const {
            TStringStream str;
            Output(str);
            return str.Str();
        }

        bool TSyncerJobTask::CheckFragmentFormat(const TString &data) {
            NSyncLog::TFragmentReader fragment(data);
            TString errorString;
            bool good = fragment.Check(errorString);
            if (!good) {
                Ctx->SyncerCtx->VCtx->Logger(NActors::NLog::PRI_ERROR, BS_SYNCER,
                       VDISKP(Ctx->SyncerCtx->VCtx->VDiskLogPrefix,
                              "TSyncerJob::CheckFragmentFormat: %s", errorString.data()));
            }
            return good;
        }

        void TSyncerJobTask::PrepareToFullRecovery(const TSyncState &syncState) {
            Y_ABORT_UNLESS(Phase == EStart || Phase == EWaitRemote, "%s", Sublog.Get().data());
            Phase = EStart;
            Type = EFullRecover;
            Current.LastSyncStatus = TSyncStatusVal::FullRecover;
            Current.SyncState = syncState;
            FullRecoverInfo = TFullRecoverInfo();
            ++RedirCounter;
        }

        TSjOutcome TSyncerJobTask::ContinueInFullRecoveryMode() {
            if (RedirCounter > 3) {
                return ReplyAndDie(TSyncStatusVal::RedirLoop);
            } else {
                return NextRequest();
            }
        }

        TSjOutcome TSyncerJobTask::ReplyAndDie(ESyncStatus status) {
            // Phase
            Phase = EFinished;

            // setup Current
            Y_ABORT_UNLESS(status != TSyncStatusVal::Running, "%s", Sublog.Get().data());
            auto now = TAppData::TimeProvider->Now();
            Current.LastSyncStatus = status;
            Current.LastTry = now;
            if (NSyncer::TPeerSyncState::Good(status))
                Current.LastGood = now;

            // log
            Ctx->SyncerCtx->VCtx->Logger(NActors::NLog::PRI_DEBUG, BS_SYNCER,
                   VDISKP(Ctx->SyncerCtx->VCtx->VDiskLogPrefix,
                          "TSyncerJob: FINISHED: status# %s",
                          NKikimrVDiskData::TSyncerVDiskEntry::ESyncStatus_Name(status).data()));

            // death
            return TSjOutcome::Death();
        }

        TSjOutcome TSyncerJobTask::NextRequest() {
            Y_ABORT_UNLESS(Phase == EStart || Phase == EWaitLocal || Phase == ETerminated,
                     "Phase# %s Log# %s", EPhaseToStr(Phase), Sublog.Get().data());

            Sublog.Log() << "NextRequest\n";

            if (Phase == ETerminated) {
                return ReplyAndDie(Current.LastSyncStatus);
            }

            if (EndOfStream) {
                Y_ABORT_UNLESS(Phase == EWaitLocal,
                         "Phase# %s Log# %s", EPhaseToStr(Phase), Sublog.Get().data());
                return ReplyAndDie(TSyncStatusVal::SyncDone);
            }

            Phase = EWaitRemote;
            ++SentMsg;
            if (Type == EFullRecover) {
                auto msg = std::make_unique<TEvBlobStorage::TEvVSyncFull>(Current.SyncState, Ctx->SelfVDiskId, VDiskId,
                    FullRecoverInfo->VSyncFullMsgsReceived, FullRecoverInfo->Stage,
                    FullRecoverInfo->LogoBlobFrom.LogoBlobID(), ReadUnaligned<ui64>(&FullRecoverInfo->BlockTabletFrom.TabletId),
                    FullRecoverInfo->BarrierFrom);
                Ctx->SyncerCtx->MonGroup.SyncerVSyncFullBytesSent() += msg->GetCachedByteSize();
                ++Ctx->SyncerCtx->MonGroup.SyncerVSyncFullMessagesSent();
                return TSjOutcome::Event(ServiceId, std::move(msg));
            } else {
                auto msg = std::make_unique<TEvBlobStorage::TEvVSync>(Current.SyncState, Ctx->SelfVDiskId, VDiskId);
                Ctx->SyncerCtx->MonGroup.SyncerVSyncBytesSent() += msg->GetCachedByteSize();
                ++Ctx->SyncerCtx->MonGroup.SyncerVSyncMessagesSent();
                return TSjOutcome::Event(ServiceId, std::move(msg));
            }
        }

        TSjOutcome TSyncerJobTask::HandleOK(
                TEvBlobStorage::TEvVSyncResult::TPtr &ev,
                const TSyncState &newSyncState,
                const TActorId &parentId)
        {
            const NKikimrBlobStorage::TEvVSyncResult &record = ev->Get()->Record;
            const TString &data = record.GetData();
            if (!CheckFragmentFormat(data)) {
                return ReplyAndDie(TSyncStatusVal::ProtocolError);
            }

            SetSyncState(newSyncState);
            EndOfStream = record.GetFinished();

            if (!data.empty()) {
                Phase = EWaitLocal;
                const TVDiskID vdisk(VDiskIDFromVDiskID(record.GetVDiskID()));
                auto syncState = GetCurrent().SyncState;
                auto msg = std::make_unique<TEvLocalSyncData>(vdisk, syncState, data);

                if (Ctx->SyncerCtx->Config->EnableLocalSyncLogDataCutting) {
                    std::unique_ptr<IActor> actor(CreateLocalSyncDataCutter(Ctx->SyncerCtx->Config,
                            Ctx->SyncerCtx->VCtx, Ctx->SyncerCtx->SkeletonId, parentId, std::move(msg)));
                    return TSjOutcome::Actor(std::move(actor), true);
                } else {

#ifdef UNPACK_LOCALSYNCDATA
                    std::unique_ptr<IActor> actor(CreateLocalSyncDataExtractor(Ctx->SyncerCtx->VCtx, Ctx->SyncerCtx->SkeletonId,
                            parentId, std::move(msg)));
                    return TSjOutcome::Actor(actor.Release(), true);
#else
                    Y_UNUSED(parentId);
                    return TSjOutcome::Event(Ctx->SyncerCtx->SkeletonId, std::move(msg));
#endif
                }
            } else {
                if (!EndOfStream) {
                    Ctx->SyncerCtx->VCtx->Logger(NActors::NLog::PRI_ERROR, BS_SYNCER,
                           VDISKP(Ctx->SyncerCtx->VCtx->VDiskLogPrefix,
                                  "TSyncerJob::HandleOK(TEvVSyncResult): "
                                  "data.empty() && !EndOfStream"));
                    return ReplyAndDie(TSyncStatusVal::ProtocolError);
                } else {
                    return ReplyAndDie(TSyncStatusVal::SyncDone);
                }
            }

        }

        TSjOutcome TSyncerJobTask::HandleRestart(const TSyncState &newSyncState) {
            SetSyncState(newSyncState);
            EndOfStream = false;
            Phase = EStart;
            Y_ABORT_UNLESS(Type == EJustSync);

            Sublog.Log() << "HandleRestart: newSyncState# " << newSyncState << "\n";
            return NextRequest();
        }

        void TSyncerJobTask::HandleStatusFlags(const NKikimrBlobStorage::TEvVSyncResult &record) {
            Y_DEBUG_ABORT_UNLESS(record.GetStatus() == NKikimrProto::OK ||
                record.GetStatus() == NKikimrProto::ALREADY ||
                record.GetStatus() == NKikimrProto::NODATA ||
                record.GetStatus() == NKikimrProto::RESTART);
            if (record.HasStatusFlags()) {
                auto flags = static_cast<NPDisk::TStatusFlags>(record.GetStatusFlags());
                TVDiskID fromVDisk = VDiskIDFromVDiskID(record.GetVDiskID());
                auto vdiskOrderNum = Ctx->SyncerCtx->VCtx->Top->GetOrderNumber(fromVDisk);
                Ctx->SyncerCtx->VCtx->GetOutOfSpaceState().Update(vdiskOrderNum, flags);
            }
        }

        TSjOutcome TSyncerJobTask::Handle(TEvBlobStorage::TEvVSyncResult::TPtr &ev, const TActorId &parentId) {
            Y_ABORT_UNLESS(Phase == EWaitRemote,
                     "Phase# %s Log# %s", EPhaseToStr(Phase), Sublog.Get().data());
            Sublog.Log() << "Handle(TEvVSyncResult): " << ev->Get()->ToString() << "\n";


            size_t bytesReceived = ev->Get()->GetCachedByteSize();
            Ctx->SyncerCtx->MonGroup.SyncerVSyncBytesReceived() += bytesReceived;
            BytesReceived += bytesReceived;

            const NKikimrBlobStorage::TEvVSyncResult &record = ev->Get()->Record;
            TVDiskID fromVDisk = VDiskIDFromVDiskID(record.GetVDiskID());
            if (!Ctx->SelfVDiskId.SameGroupAndGeneration(fromVDisk)) {
                return ReplyAndDie(TSyncStatusVal::Race);
            }

            TSyncState newSyncState = SyncStateFromSyncState(record.GetNewSyncState());
            Ctx->SyncerCtx->VCtx->Logger(NActors::NLog::PRI_DEBUG, BS_SYNCER,
                    VDISKP(Ctx->SyncerCtx->VCtx->VDiskLogPrefix,
                        "SYNCER_VSYNCRES: fromVDisk# %s newSyncState# %s",
                        fromVDisk.ToString().data(), newSyncState.ToString().data()));
            // status check
            NKikimrProto::EReplyStatus status = record.GetStatus();
            switch (status) {
                case NKikimrProto::OK:
                    HandleStatusFlags(record);
                    return HandleOK(ev, newSyncState, parentId);
                case NKikimrProto::RACE:
                    return ReplyAndDie(TSyncStatusVal::Race);
                case NKikimrProto::ERROR:
                    return ReplyAndDie(TSyncStatusVal::Error);
                case NKikimrProto::ALREADY:
                    HandleStatusFlags(record);
                    SetSyncState(newSyncState);
                    return ReplyAndDie(TSyncStatusVal::DisksSynced);
                case NKikimrProto::NODATA:
                    HandleStatusFlags(record);
                    PrepareToFullRecovery(newSyncState);
                    return ContinueInFullRecoveryMode();
                case NKikimrProto::NOTREADY:
                    return ReplyAndDie(TSyncStatusVal::NotReady);
                case NKikimrProto::BLOCKED:
                    return ReplyAndDie(TSyncStatusVal::Locked);
                case NKikimrProto::RESTART:
                    HandleStatusFlags(record);
                    return HandleRestart(newSyncState);
                default: {
                    Ctx->SyncerCtx->VCtx->Logger(NActors::NLog::PRI_ERROR, BS_SYNCER,
                           VDISKP(Ctx->SyncerCtx->VCtx->VDiskLogPrefix,
                                  "TSyncerJob::Handle(TEvVSyncResult): status# %s",
                                  NKikimrProto::EReplyStatus_Name(status).data()));
                    return ReplyAndDie(TSyncStatusVal::ProtocolError);
                }
            }
        }

        TSjOutcome TSyncerJobTask::HandleOK(
                TEvBlobStorage::TEvVSyncFullResult::TPtr &ev,
                const TSyncState &syncState,
                const TActorId &parentId)
        {
            const NKikimrBlobStorage::TEvVSyncFullResult &record = ev->Get()->Record;
            const TString &data = record.GetData();
            if (!CheckFragmentFormat(data)) {
                return ReplyAndDie(TSyncStatusVal::ProtocolError);
            }

            // update stage and current keys
            EndOfStream = record.GetFinished();
            FullRecoverInfo->Stage = record.GetStage();
            FullRecoverInfo->LogoBlobFrom = LogoBlobIDFromLogoBlobID(record.GetLogoBlobFrom());
            FullRecoverInfo->BlockTabletFrom = record.GetBlockTabletFrom();
            FullRecoverInfo->BarrierFrom = TKeyBarrier(record.GetBarrierFrom());

            if (FullRecoverInfo->VSyncFullMsgsReceived == 1) {
                SetSyncState(syncState); // from now keep this position in memory
            } else {
                Y_ABORT_UNLESS(FullRecoverInfo->VSyncFullMsgsReceived > 1,
                         "Phase# %s Log# %s", EPhaseToStr(Phase), Sublog.Get().data());
                Y_ABORT_UNLESS(GetCurrent().SyncState == syncState,
                         "Phase# %s Log# %s", EPhaseToStr(Phase), Sublog.Get().data());
            }

            if (!data.empty()) {
                const TVDiskID vdisk(VDiskIDFromVDiskID(record.GetVDiskID()));
                // While working on full recovery we put OldSyncState to the revery log,
                // while keeping in memory correct SyncState obtained from remote vdisk;
                // the correct SyncState is written by committer only once. We required
                // to work this way because records we get from remote node are not ordered
                // by lsn, so we need to get them all at once and finally write the correct
                // SyncState position.
                Phase = EWaitLocal;
                auto msg = std::make_unique<TEvLocalSyncData>(vdisk, OldSyncState, data);

                if (Ctx->SyncerCtx->Config->EnableLocalSyncLogDataCutting) {
                    std::unique_ptr<IActor> actor(CreateLocalSyncDataCutter(Ctx->SyncerCtx->Config,
                            Ctx->SyncerCtx->VCtx, Ctx->SyncerCtx->SkeletonId, parentId, std::move(msg)));
                    return TSjOutcome::Actor(std::move(actor), true);
                } else {
                    auto msg = std::make_unique<TEvLocalSyncData>(vdisk, OldSyncState, data);
#ifdef UNPACK_LOCALSYNCDATA
                    std::unique_ptr<IActor> actor(CreateLocalSyncDataExtractor(Ctx->SyncerCtx->VCtx, Ctx->SyncerCtx->SkeletonId,
                            parentId, std::move(msg)));
                    return TSjOutcome::Actor(actor.Release(), true);
#else
                    Y_UNUSED(parentId);
                    return TSjOutcome::Event(Ctx->SyncerCtx->SkeletonId, std::move(msg));
#endif
                }
            } else {
                if (!EndOfStream) {
                    Ctx->SyncerCtx->VCtx->Logger(NActors::NLog::PRI_ERROR, BS_SYNCER,
                           VDISKP(Ctx->SyncerCtx->VCtx->VDiskLogPrefix,
                                  "TSyncerJob::HandleOK(TEvVSyncFullResult): "
                                  "data.empty() && !EndOfStream"));
                    return ReplyAndDie(TSyncStatusVal::ProtocolError);
                } else {
                    return ReplyAndDie(TSyncStatusVal::SyncDone);
                }
            }
        }

        TSjOutcome TSyncerJobTask::Handle(TEvBlobStorage::TEvVSyncFullResult::TPtr &ev, const TActorId &parentId) {
            Y_ABORT_UNLESS(Phase == EWaitRemote,
                     "Phase# %s Log# %s", EPhaseToStr(Phase), Sublog.Get().data());
            Sublog.Log() << "Handle(TEvVSyncFullResult): " << ev->Get()->ToString() << "\n";

            size_t bytesReceived = ev->Get()->GetCachedByteSize();
            Ctx->SyncerCtx->MonGroup.SyncerVSyncFullBytesReceived() += bytesReceived;
            BytesReceived += bytesReceived;

            const NKikimrBlobStorage::TEvVSyncFullResult &record = ev->Get()->Record;
            Y_ABORT_UNLESS(record.GetCookie() == FullRecoverInfo->VSyncFullMsgsReceived,
                     "Phase# %s Log# %s", EPhaseToStr(Phase), Sublog.Get().data());
            FullRecoverInfo->VSyncFullMsgsReceived++;
            TVDiskID fromVDisk = VDiskIDFromVDiskID(record.GetVDiskID());
            if (!Ctx->SelfVDiskId.SameGroupAndGeneration(fromVDisk)) {
                return ReplyAndDie(TSyncStatusVal::Race);
            }

            TSyncState syncState = SyncStateFromSyncState(record.GetSyncState());
            Ctx->SyncerCtx->VCtx->Logger(NActors::NLog::PRI_DEBUG, BS_SYNCER,
                    VDISKP(Ctx->SyncerCtx->VCtx->VDiskLogPrefix,
                        "SYNCER_VSYNCFULLRES: fromVDisk# %s syncState# %s",
                        fromVDisk.ToString().data(), syncState.ToString().data()));

            // status check
            NKikimrProto::EReplyStatus status = record.GetStatus();
            switch (status) {
                case NKikimrProto::OK:
                    return HandleOK(ev, syncState, parentId);
                case NKikimrProto::RACE:
                    return ReplyAndDie(TSyncStatusVal::Race);
                case NKikimrProto::ERROR:
                    return ReplyAndDie(TSyncStatusVal::Error);
                case NKikimrProto::NODATA:
                    // Disk guid changed (again?) and we need to start from the beginning
                    PrepareToFullRecovery(syncState);
                    return ContinueInFullRecoveryMode();
                case NKikimrProto::NOTREADY:
                    return ReplyAndDie(TSyncStatusVal::NotReady);
                case NKikimrProto::BLOCKED:
                    return ReplyAndDie(TSyncStatusVal::Locked);
                default: {
                    Ctx->SyncerCtx->VCtx->Logger(NActors::NLog::PRI_ERROR, BS_SYNCER,
                           VDISKP(Ctx->SyncerCtx->VCtx->VDiskLogPrefix,
                                  "TSyncerJob::Handle(TEvVSyncFullResult): status# %s",
                                  NKikimrProto::EReplyStatus_Name(status).data()));
                    return ReplyAndDie(TSyncStatusVal::ProtocolError);
                }
            }
        }

        TSjOutcome TSyncerJobTask::Handle(TEvLocalSyncDataResult::TPtr &ev) {
            Y_ABORT_UNLESS(Phase == EWaitLocal || Phase == ETerminated,
                     "Phase# %s Log# %s", EPhaseToStr(Phase), Sublog.Get().data());
            Sublog.Log() << "Handle(TEvLocalSyncDataResult): " << ev->Get()->ToString() << "\n";

            if (ev->Get()->Status == NKikimrProto::OUT_OF_SPACE) {
                // no space
                return ReplyAndDie(TSyncStatusVal::OutOfSpace);
            }
            Y_ABORT_UNLESS(ev->Get()->Status == NKikimrProto::OK,
                     "msg# %s Phase# %s Log# %s", ev->Get()->ToString().data(), EPhaseToStr(Phase), Sublog.Get().data());

            if (EndOfStream) {
                return ReplyAndDie(TSyncStatusVal::SyncDone);
            } else {
                if (Phase == ETerminated) {
                    // job timed out
                    return ReplyAndDie(Current.LastSyncStatus);
                } else {
                    // continue sending requests
                    return NextRequest();
                }
            }
        }

        TSjOutcome TSyncerJobTask::Terminate(ESyncStatus status) {
            Y_ABORT_UNLESS(Phase == EWaitLocal || Phase == EWaitRemote || Phase == EStart,
                     "Phase# %s Log# %s", EPhaseToStr(Phase), Sublog.Get().data());
            Sublog.Log() << "Terminate: status# " << status << "\n";
            Phase = ETerminated;
            Current.LastSyncStatus = status;

            // we can get job timeout or session disconnect during local write;
            // we have to wait until this write is finished (to avoid generating infinite writes)
            if (Phase == EWaitLocal) {
                // do nothing, wait for TEvLocalSyncDataResult
                return TSjOutcome::Nothing();
            } else {
                return ReplyAndDie(status);
            }
        }

    } // NSyncer
} // NKikimr
