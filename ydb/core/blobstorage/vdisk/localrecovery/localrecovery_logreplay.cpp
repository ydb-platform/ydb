#include "localrecovery_logreplay.h"
#include "localrecovery_defs.h"
#include "localrecovery_readbulksst.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_lsnmngr.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_hugeblobctx.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer_data.h>
#include <ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer_localwriter.h>
#include <ydb/core/blobstorage/vdisk/hulldb/recovery/hulldb_recovery.h>
#include <ydb/core/blobstorage/vdisk/hullop/hullop_entryserialize.h>
#include <ydb/core/blobstorage/vdisk/anubis_osiris/blobstorage_anubis_osiris.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogmsgreader.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogrecovery.h>

using namespace NKikimrServices;
using namespace NKikimr::NSyncLog;
using namespace NKikimr::NHuge;

namespace NKikimr {

    TLocalRecoveryContext::TLocalRecoveryContext(TIntrusivePtr<TVDiskContext> vctx)
        : VCtx(std::move(vctx))
        , MonGroup(VCtx->VDiskCounters, "subsystem", "localdbrecovery")
        , RecovInfo(MakeIntrusive<TLocalRecoveryInfo>(MonGroup))
    {}

    TLocalRecoveryContext::~TLocalRecoveryContext() = default;

    ////////////////////////////////////////////////////////////////////////////
    // TRecoveryLogReplayer
    // Logic for recovery log replay
    ////////////////////////////////////////////////////////////////////////////
    class TRecoveryLogReplayer : public TActorBootstrapped<TRecoveryLogReplayer> {
        friend class TActorBootstrapped<TRecoveryLogReplayer>;

        enum class EDispatchStatus {
            Success = 0,
            Error = 1,
            Async = 2
        };

        struct TReadLogResultCtx {
            // message being handle
            NPDisk::TEvReadLogResult::TPtr Ev;
            // shortcut to Ev content
            NPDisk::TEvReadLogResult *Msg = nullptr;
            ui64 Index = 0;
            ui64 VecSize = 0;

            TReadLogResultCtx(NPDisk::TEvReadLogResult::TPtr ev)
                : Ev(ev)
                , Msg(Ev->Get())
                , VecSize(Msg->Results.size())
            {}
        };

        const TActorId ParentId;
        std::shared_ptr<TLocalRecoveryContext> LocRecCtx;
        TActiveActors ActiveActors;
        NPDisk::TLogPosition PrevLogPos = {0, 0};
        std::unique_ptr<TReadLogResultCtx> ReadLogCtx;

        ui64 RecoveredLsn = 0;
        ui64 SyncLogMaxLsnStored = 0;

        // fields for pb messages defined once
        NKikimrBlobStorage::TEvVPut PutMsg;
        TPutRecoveryLogRecOpt PutMsgOpt;
        TAnubisOsirisPutRecoveryLogRec AnubisOsirisPutMsg;
        NKikimrBlobStorage::TEvVBlock BlockMsg;
        NKikimrBlobStorage::TEvVCollectGarbage GCMsg;
        TEvLocalSyncData LocalSyncDataMsg;
        NKikimrBlobStorage::THandoffDelLogoBlob HandoffDelMsg;
        NHuge::TAllocChunkRecoveryLogRec HugeBlobAllocChunkRecoveryLogRec;
        NHuge::TFreeChunkRecoveryLogRec HugeBlobFreeChunkRecoveryLogRec;
        NHuge::TPutRecoveryLogRec HugeBlobPutRecoveryLogRec;
        TDiskPartVec HugeBlobs;
        NKikimrVDiskData::TPhantomLogoBlobs PhantomLogoBlobs;

        void Bootstrap(const TActorContext &ctx) {
            LOG_NOTICE(ctx, BS_LOCALRECOVERY,
                       VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                            "TRecoveryLogReplayer: START"));

            Become(&TThis::StateFunc);
            SendReadLogRequest(ctx, NPDisk::TLogPosition {0, 0});
        }

        void HandlePoison(const TActorContext &ctx) {
            ActiveActors.KillAndClear(ctx);
            Die(ctx);
        }

        void SendReadLogRequest(const TActorContext &ctx, NPDisk::TLogPosition pos) {
            PrevLogPos = pos;
            ctx.Send(LocRecCtx->PDiskCtx->PDiskId,
                new NPDisk::TEvReadLog(LocRecCtx->PDiskCtx->Dsk->Owner, LocRecCtx->PDiskCtx->Dsk->OwnerRound,
                        PrevLogPos));
        }

        void Finish(const TActorContext &ctx, NKikimrProto::EReplyStatus status, const TString &errorReason) {
            ctx.Send(ParentId, new TEvRecoveryLogReplayDone(status, errorReason, RecoveredLsn));
            Die(ctx);
        }

        void Handle(NPDisk::TEvReadLogResult::TPtr &ev, const TActorContext &ctx) {
            ReadLogCtx = std::make_unique<TReadLogResultCtx>(ev);

            if (ReadLogCtx->Msg->Status != NKikimrProto::OK) {
                Finish(ctx, ReadLogCtx->Msg->Status, "Recovery log read failed");
                return;
            } else {
                Y_ABORT_UNLESS(ReadLogCtx->Msg->Position == PrevLogPos);
                // update RecovInfo
                LocRecCtx->RecovInfo->HandleReadLogResult(ReadLogCtx->Msg->Results);
                // run dispatcher
                InterruptableLogRecordsDispatch(ctx);
            }
        }

        void InterruptableLogRecordsDispatch(const TActorContext &ctx) {
            // dispatch log records
            while (ReadLogCtx->Index < ReadLogCtx->VecSize) {
                EDispatchStatus status = DispatchLogRecord(ctx, ReadLogCtx->Msg->Results[ReadLogCtx->Index++]);
                switch (status) {
                    case EDispatchStatus::Success:
                        break;
                    case EDispatchStatus::Error:
                        Finish(ctx, NKikimrProto::ERROR, "Error dispatching recovery log record");
                        return;
                    case EDispatchStatus::Async:
                        return; // wait for async call
                    default:
                        Y_ABORT("Unexpected case");
                }
            }

            if (!ReadLogCtx->Msg->IsEndOfLog) {
                // continue reading and applying log
                SendReadLogRequest(ctx, ReadLogCtx->Msg->NextPosition);
                // dispatching finished
                ReadLogCtx.reset();
            } else {
                // dispatching finished
                ReadLogCtx.reset();
                // end
                LocRecCtx->RecovInfo->FinishDispatching();
                LocRecCtx->RepairedHuge->FinishRecovery(ctx);
                VerifyOwnedChunks(ctx);

                LocRecCtx->VCtx->LocalRecoveryErrorStr = "";
                Finish(ctx, NKikimrProto::OK, {});
            }
        }

        void PutLogoBlobToHull(const TActorContext &ctx,
                               ui64 lsn,
                               const TLogoBlobID &id,
                               const TIngress &ingress,
                               const TString &buf,
                               bool fromVPutCommand) {
            // skip records that already in index
            if (LocRecCtx->HullDbRecovery->GetHullDs()->LogoBlobs->SkipRecord(lsn)) {
                LocRecCtx->RecovInfo->FreshSkipLogoBlob();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (LOGOBLOB) SKIPPED: lsn# %" PRIu64 " id# %s",
                                lsn, id.ToString().data()));
            } else {
                LocRecCtx->RecovInfo->FreshApplyLogoBlob();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (LOGOBLOB) ADDED: lsn# %" PRIu64 " id# %s",
                                lsn, id.ToString().data()));
                TLogoBlobID genId(id, 0);
                if (fromVPutCommand)
                    LocRecCtx->HullDbRecovery->ReplayAddLogoBlobCmd(ctx, genId, id.PartId(), ingress, TRope(buf), lsn,
                            THullDbRecovery::RECOVERY);
                else
                    LocRecCtx->HullDbRecovery->ReplayAddLogoBlobCmd(ctx, genId, ingress, lsn,
                            THullDbRecovery::RECOVERY);
            }
        }

        void PutLogoBlobToHullAndSyncLog(const TActorContext &ctx,
                                         ui64 lsn,
                                         const TLogoBlobID &id,
                                         const TIngress &ingress,
                                         const TString &buf,
                                         bool fromVPutCommand) {
            // put logoblob to Hull Db
            PutLogoBlobToHull(ctx, lsn, id, ingress, buf, fromVPutCommand);

            // skip records that already in synclog
            if (lsn <= SyncLogMaxLsnStored) {
                LocRecCtx->RecovInfo->SyncLogSkipLogoBlob();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (SYNCLOG LOGOBLOB) SKIPPED: lsn# %" PRIu64 " id# %s",
                                lsn, id.ToString().data()));
            } else {
                LocRecCtx->RecovInfo->SyncLogApplyLogoBlob();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (SYNCLOG LOGOBLOB) ADDED: lsn# %" PRIu64 " id# %s",
                                lsn, id.ToString().data()));
                LocRecCtx->SyncLogRecovery->PutLogoBlob(LocRecCtx->VCtx->Top->GType, lsn, TLogoBlobID(id, 0), ingress);
            }
        }

        void PutHugeLogoBlobToHullAndSyncLog(const TActorContext &ctx,
                                             ui64 lsn,
                                             const TLogoBlobID &id,
                                             const TIngress &ingress,
                                             const TDiskPart &diskAddr) {
            // skip records that already in index
            if (LocRecCtx->HullDbRecovery->GetHullDs()->LogoBlobs->SkipRecord(lsn)) {
                LocRecCtx->RecovInfo->FreshSkipHugeLogoBlob();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (HUGELOGOBLOB) SKIPPED: lsn# %" PRIu64 " id# %s",
                                lsn, id.ToString().data()));
            } else {
                LocRecCtx->RecovInfo->FreshApplyHugeLogoBlob();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (HUGELOGOBLOB) ADDED: lsn# %" PRIu64 " id# %s",
                                lsn, id.ToString().data()));
                TLogoBlobID genId(id, 0);
                LocRecCtx->HullDbRecovery->ReplayAddHugeLogoBlobCmd(ctx, genId, ingress, diskAddr, lsn,
                        THullDbRecovery::RECOVERY);
            }

            // skip records that already in synclog
            if (lsn <= SyncLogMaxLsnStored) {
                LocRecCtx->RecovInfo->SyncLogSkipHugeLogoBlob();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (SYNCLOG HUGELOGOBLOB) SKIPPED: lsn# %" PRIu64 " id# %s",
                                lsn, id.ToString().data()));
            } else {
                LocRecCtx->RecovInfo->SyncLogApplyHugeLogoBlob();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (SYNCLOG HUGELOGOBLOB) ADDED: lsn# %" PRIu64 " id# %s",
                                lsn, id.ToString().data()));
                LocRecCtx->SyncLogRecovery->PutLogoBlob(LocRecCtx->VCtx->Top->GType, lsn, TLogoBlobID(id, 0), ingress);
            }
        }

        void PutBlockToHull(const TActorContext &ctx, ui64 lsn, ui64 tabletId, ui32 gen, ui64 issuerGuid) {
            // skip records that already in index
            if (LocRecCtx->HullDbRecovery->GetHullDs()->Blocks->SkipRecord(lsn)) {
                LocRecCtx->RecovInfo->FreshSkipBlock();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (BLOCK) SKIPPED: lsn# %" PRIu64 " tabletId# %" PRIu64
                                " gen# %" PRIu32, lsn, tabletId, gen));
            } else {
                LocRecCtx->RecovInfo->FreshApplyBlock();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (BLOCK) ADDED: lsn# %" PRIu64 " tabletId# %" PRIu64
                                " gen# %" PRIu32, lsn, tabletId, gen));
                LocRecCtx->HullDbRecovery->ReplayAddBlockCmd(ctx, tabletId, gen, issuerGuid, lsn, THullDbRecovery::RECOVERY);
            }
        }

        void PutBlockToHullAndSyncLog(const TActorContext &ctx, ui64 lsn, ui64 tabletId, ui32 gen, ui64 issuerGuid) {
            // put block to Hull Db
            PutBlockToHull(ctx, lsn, tabletId, gen, issuerGuid);

            // skip records that already in synclog
            if (lsn <= SyncLogMaxLsnStored) {
                LocRecCtx->RecovInfo->SyncLogSkipBlock();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (SYNCLOG BLOCK) SKIPPED: lsn# %" PRIu64 " tabletId# %" PRIu64
                                " gen# %" PRIu32, lsn, tabletId, gen));
            } else {
                LocRecCtx->RecovInfo->SyncLogApplyBlock();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (SYNCLOG BLOCK) ADDED: lsn# %" PRIu64 " tabletId# %" PRIu64
                                " gen# %" PRIu32, lsn, tabletId, gen));
                LocRecCtx->SyncLogRecovery->PutBlock(lsn, tabletId, gen);
            }
        }

        void PutGC(const TActorContext &ctx, ui64 lsn,
                   const NKikimrBlobStorage::TEvVCollectGarbage &gcmsg,
                   const TBarrierIngress &ingress) {
            // NOTE: we update barriers and logoblobs dbs separately, because they compact independantly

            // skip records that already in index
            if (LocRecCtx->HullDbRecovery->GetHullDs()->Barriers->SkipRecord(lsn)) {
                LocRecCtx->RecovInfo->FreshSkipGCBarrier();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (GC BARRIER) SKIPPED: lsn# %" PRIu64, lsn));
            } else {
                LocRecCtx->RecovInfo->FreshApplyGCBarrier();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (GC BARRIER) ADDED: lsn# %" PRIu64, lsn));
                LocRecCtx->HullDbRecovery->ReplayAddGCCmd_BarrierSubcommand(ctx, gcmsg, ingress, lsn,
                        THullDbRecovery::RECOVERY);
            }


            // skip records that already in index
            if (LocRecCtx->HullDbRecovery->GetHullDs()->LogoBlobs->SkipRecord(lsn)) {
                LocRecCtx->RecovInfo->FreshSkipGCLogoBlob();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (GC LOGOBLOB) SKIPPED: lsn# %" PRIu64, lsn));
            } else {
                LocRecCtx->RecovInfo->FreshApplyGCLogoBlob();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (GC LOGOBLOB) ADDED: lsn# %" PRIu64, lsn));
                LocRecCtx->HullDbRecovery->ReplayAddGCCmd_LogoBlobsSubcommand(ctx, gcmsg, lsn,
                        THullDbRecovery::RECOVERY);
            }


            // skip records that already in synclog
            if (lsn <= SyncLogMaxLsnStored) {
                LocRecCtx->RecovInfo->SyncLogSkipGC();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (SYNCLOG BARRIER) SKIPPED: lsn# %" PRIu64, lsn));
            } else {
                LocRecCtx->RecovInfo->SyncLogApplyGC();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (SYNCLOG BARRIER) ADDED: lsn# %" PRIu64, lsn));
                LocRecCtx->SyncLogRecovery->PutGC(LocRecCtx->VCtx->Top->GType, lsn, gcmsg, ingress);
            }
        }

        void PutBarrierToHull(const TActorContext &ctx,
                              ui64 lsn,
                              ui64 tabletId,
                              ui32 channel,
                              ui32 gen,
                              ui32 genCounter,
                              ui32 collectGen,
                              ui32 collectStep,
                              bool hard,
                              const TBarrierIngress &ingress) {
            // skip records that already in index
            if (LocRecCtx->HullDbRecovery->GetHullDs()->Barriers->SkipRecord(lsn)) {
                LocRecCtx->RecovInfo->FreshSkipBarrier();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (BARRIER) SKIPPED: lsn# %" PRIu64
                                " tabletId# %" PRIu64 " channel# %" PRIu32
                                " gen# %" PRIu32 " genCounter# %" PRIu32
                                " collectGen# %" PRIu32 " collectStep# %" PRIu32
                                " hard# %s", lsn, tabletId, channel, gen, genCounter,
                                collectGen, collectStep, hard ? "true" : "false"));
            } else {
                LocRecCtx->RecovInfo->FreshApplyBarrier();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (BARRIER) ADDED: lsn# %" PRIu64
                                " tabletId# %" PRIu64 " channel# %" PRIu32
                                " gen# %" PRIu32 " genCounter# %" PRIu32
                                " collectGen# %" PRIu32 " collectStep# %" PRIu32
                                " hard# %s", lsn, tabletId, channel, gen, genCounter,
                                collectGen, collectStep, hard ? "true" : "false"));
                LocRecCtx->HullDbRecovery->ReplayAddBarrierCmd(ctx, tabletId, channel,
                        gen, genCounter, collectGen, collectStep, hard, ingress, lsn, THullDbRecovery::RECOVERY);
            }
        }

        EDispatchStatus HandleLogoBlob(const TActorContext &ctx, const NPDisk::TLogRecord &record) {
            bool success = PutMsg.ParseFromArray(record.Data.GetData(), record.Data.GetSize());
            if (!success)
                return EDispatchStatus::Error;

            const bool fromVPutCommand = true;
            const TLogoBlobID id = LogoBlobIDFromLogoBlobID(PutMsg.GetBlobID());
            const TString &buf = PutMsg.GetBuffer();
            TMaybe<TIngress> ingress = TIngress::CreateIngressWithLocal(LocRecCtx->VCtx->Top.get(), LocRecCtx->VCtx->ShortSelfVDisk, id);
            Y_VERIFY_S(ingress, "Failed to create ingress, VDiskId# " << LocRecCtx->VCtx->ShortSelfVDisk << ", BlobId# " << id);

            PutLogoBlobToHullAndSyncLog(ctx, record.Lsn, id, *ingress, buf, fromVPutCommand);
            return EDispatchStatus::Success;
        }

        EDispatchStatus HandleOptLogoBlob(const TActorContext &ctx, const NPDisk::TLogRecord &record) {
            bool success = PutMsgOpt.ParseFromArray(LocRecCtx->VCtx->Top->GType, record.Data.GetData(), record.Data.GetSize());
            if (!success)
                return EDispatchStatus::Error;

            const bool fromVPutCommand = true;
            TMaybe<TIngress> ingress = TIngress::CreateIngressWithLocal(LocRecCtx->VCtx->Top.get(), LocRecCtx->VCtx->ShortSelfVDisk,
                PutMsgOpt.Id);
            Y_VERIFY_S(ingress, "Failed to create ingress, VDiskId# " << LocRecCtx->VCtx->ShortSelfVDisk << 
                    ", BlobId# " << PutMsgOpt.Id);

            PutLogoBlobToHullAndSyncLog(ctx, record.Lsn, PutMsgOpt.Id, *ingress, PutMsgOpt.Data, fromVPutCommand);
            return EDispatchStatus::Success;
        }

        EDispatchStatus HandleBlock(const TActorContext &ctx, const NPDisk::TLogRecord &record) {
            bool success = BlockMsg.ParseFromArray(record.Data.GetData(), record.Data.GetSize());
            if (!success)
                return EDispatchStatus::Error;

            const ui64 tabletId = BlockMsg.GetTabletId();
            const ui32 gen = BlockMsg.GetGeneration();
            PutBlockToHullAndSyncLog(ctx, record.Lsn, tabletId, gen, BlockMsg.GetIssuerGuid());
            return EDispatchStatus::Success;
        }

        EDispatchStatus HandleGC(const TActorContext &ctx, const NPDisk::TLogRecord &record) {
            bool success = GCMsg.ParseFromArray(record.Data.GetData(), record.Data.GetSize());
            if (!success)
                return EDispatchStatus::Error;

            TBarrierIngress ingress(LocRecCtx->HullCtx->IngressCache.Get());
            PutGC(ctx, record.Lsn, GCMsg, ingress);
            return EDispatchStatus::Success;
        }

        void ApplySyncDataByRecord(const TActorContext &ctx, ui64 recordLsn) {
            // count number of records
            NSyncLog::TFragmentReader fragment(LocalSyncDataMsg.Data);
            std::vector<const NSyncLog::TRecordHdr*> records = fragment.ListRecords();
            ui64 recsNum = records.size();

            // calculate lsn
            Y_DEBUG_ABORT_UNLESS(recordLsn >= recsNum, "recordLsn# %" PRIu64 " recsNum# %" PRIu64,
                           recordLsn, recsNum);
            ui64 lsn = recordLsn - recsNum + 1;

            // record handlers
            auto blobHandler = [&] (const NSyncLog::TLogoBlobRec *rec) {
                LocRecCtx->RecovInfo->SyncDataTryPutLogoBlob();
                const bool fromVPutCommand = false;
                PutLogoBlobToHull(ctx, lsn, rec->LogoBlobID(), rec->Ingress, TString(), fromVPutCommand);
                lsn++;
            };
            auto blockHandler = [&] (const NSyncLog::TBlockRec *rec) {
                LocRecCtx->RecovInfo->SyncDataTryPutBlock();
                PutBlockToHull(ctx, lsn, rec->TabletId, rec->Generation, 0);
                lsn++;
            };
            auto barrierHandler = [&] (const NSyncLog::TBarrierRec *rec) {
                LocRecCtx->RecovInfo->SyncDataTryPutBarrier();
                PutBarrierToHull(ctx, lsn, rec->TabletId, rec->Channel, rec->Gen,
                                 rec->GenCounter, rec->CollectGeneration,
                                 rec->CollectStep, rec->Hard, rec->Ingress);
                lsn++;
            };
            auto blockHandlerV2 = [&](const NSyncLog::TBlockRecV2 *rec) {
                LocRecCtx->RecovInfo->SyncDataTryPutBlock();
                PutBlockToHull(ctx, lsn, rec->TabletId, rec->Generation, rec->IssuerGuid);
                lsn++;
            };

            // apply local sync data
            for (const NSyncLog::TRecordHdr* rec : records) {
                NSyncLog::HandleRecordHdr(rec, blobHandler, blockHandler, barrierHandler, blockHandlerV2);
            }
        }

        void PutLogoBlobsBatchToHull(
                const TActorContext &ctx,
                std::shared_ptr<TFreshAppendixLogoBlobs> &&logoBlobs,
                TLsnSeg seg)
        {
            // skip records that already in index
            if (LocRecCtx->HullDbRecovery->GetHullDs()->LogoBlobs->SkipRecord(seg.Last)) {
                LocRecCtx->RecovInfo->FreshSkipLogoBlobsBatch();
                LOG_DEBUG_S(ctx, BS_LOCALRECOVERY, LocRecCtx->VCtx->VDiskLogPrefix
                        << "RECORD (LOGOBLOBS_BATCH) SKIPPED: lsn# " << seg);
            } else {
                LocRecCtx->RecovInfo->FreshApplyLogoBlobsBatch();
                LOG_DEBUG_S(ctx, BS_LOCALRECOVERY, LocRecCtx->VCtx->VDiskLogPrefix
                        << "RECORD (LOGOBLOBS_BATCH) ADDED: lsn# " << seg);

                LocRecCtx->HullDbRecovery->ReplaySyncDataCmd_LogoBlobsBatch(ctx, std::move(logoBlobs),
                        seg, THullDbRecovery::RECOVERY);
            }
        }

        void PutBlocksBatchToHull(
                const TActorContext &ctx,
                std::shared_ptr<TFreshAppendixBlocks> &&blocks,
                TLsnSeg seg)
        {
            // skip records that already in index
            if (LocRecCtx->HullDbRecovery->GetHullDs()->Blocks->SkipRecord(seg.Last)) {
                LocRecCtx->RecovInfo->FreshSkipBlocksBatch();
                LOG_DEBUG_S(ctx, BS_LOCALRECOVERY, LocRecCtx->VCtx->VDiskLogPrefix
                        << "RECORD (BLOCKS_BATCH) SKIPPED: lsn# " << seg);
            } else {
                LocRecCtx->RecovInfo->FreshApplyBlocksBatch();
                LOG_DEBUG_S(ctx, BS_LOCALRECOVERY, LocRecCtx->VCtx->VDiskLogPrefix
                        << "RECORD (BLOCKS_BATCH) ADDED: lsn# " << seg);

                LocRecCtx->HullDbRecovery->ReplaySyncDataCmd_BlocksBatch(ctx, std::move(blocks),
                        seg, THullDbRecovery::RECOVERY);
            }
        }

        void PutBarriersBatchToHull(
                const TActorContext &ctx,
                std::shared_ptr<TFreshAppendixBarriers> &&barriers,
                TLsnSeg seg)
        {
            // skip records that already in index
            if (LocRecCtx->HullDbRecovery->GetHullDs()->Barriers->SkipRecord(seg.Last)) {
                LocRecCtx->RecovInfo->FreshSkipBarriersBatch();
                LOG_DEBUG_S(ctx, BS_LOCALRECOVERY, LocRecCtx->VCtx->VDiskLogPrefix
                        << "RECORD (BARRIERS_BATCH) SKIPPED: lsn# " << seg);
            } else {
                LocRecCtx->RecovInfo->FreshApplyBarriersBatch();
                LOG_DEBUG_S(ctx, BS_LOCALRECOVERY, LocRecCtx->VCtx->VDiskLogPrefix
                        << "RECORD (BARRIERS_BATCH) ADDED: lsn# " << seg);
                LocRecCtx->HullDbRecovery->ReplaySyncDataCmd_BarriersBatch(ctx, std::move(barriers),
                        seg, THullDbRecovery::RECOVERY);
            }
        }

        void ApplySyncDataViaBatch(const TActorContext &ctx, ui64 recordLsn) {
            LocalSyncDataMsg.UnpackData(LocRecCtx->VCtx);

            auto &freshBatch = LocalSyncDataMsg.Extracted;
            ui64 curLsn = recordLsn - freshBatch.GetRecords() + 1;
            if (freshBatch.LogoBlobs) {
                const ui64 logoBlobsCount = freshBatch.LogoBlobs->GetSize();
                PutLogoBlobsBatchToHull(ctx, std::move(freshBatch.LogoBlobs), {curLsn, curLsn + logoBlobsCount - 1});
                curLsn += logoBlobsCount;
            }

            if (freshBatch.Blocks) {
                const ui64 blocksCount = freshBatch.Blocks->GetSize();
                PutBlocksBatchToHull(ctx, std::move(freshBatch.Blocks), {curLsn, curLsn + blocksCount - 1});
                curLsn += blocksCount;
            }

            if (freshBatch.Barriers) {
                const ui64 barriersCount = freshBatch.Barriers->GetSize();
                PutBarriersBatchToHull(ctx, std::move(freshBatch.Barriers), {curLsn, curLsn + barriersCount - 1});
                curLsn += barriersCount;
            }
        }

        EDispatchStatus HandleSyncData(const TActorContext &ctx, const NPDisk::TLogRecord &record) {
            TMemoryInput str(record.Data.GetData(), record.Data.GetSize());
            bool success = LocalSyncDataMsg.Deserialize(str);
            if (!success)
                return EDispatchStatus::Error;

            // data to apply
#ifdef UNPACK_LOCALSYNCDATA
            ApplySyncDataViaBatch(ctx, record.Lsn);
#else
            ApplySyncDataByRecord(ctx, record.Lsn);
#endif

            if (LocRecCtx->SyncerData.Get()) {
                // recovery is done in single thread
                LocRecCtx->SyncerData->PutFromRecoveryLog(LocalSyncDataMsg.VDiskID, LocalSyncDataMsg.SyncState);
            }

            return EDispatchStatus::Success;
        }

        EDispatchStatus HandleHandoffDel(const TActorContext &ctx, const NPDisk::TLogRecord &record) {
            bool success = HandoffDelMsg.ParseFromArray(record.Data.GetData(), record.Data.GetSize());
            if (!success)
                return EDispatchStatus::Error;

            const ui64 lsn = record.Lsn;
            const TLogoBlobID id = LogoBlobIDFromLogoBlobID(HandoffDelMsg.GetBlobID());
            const TIngress ingress(HandoffDelMsg.GetIngress());

            // we don't add handoff delete commands to the hull

            // skip records that already in synclog
            if (lsn <= SyncLogMaxLsnStored) {
                LocRecCtx->RecovInfo->FreshSkipHandoffDel();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (SYNCLOG HNDOFF) SKIPPED: lsn# %" PRIu64, lsn));
            } else {
                LocRecCtx->RecovInfo->FreshApplyHandoffDel();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (SYNCLOG HNDOFF) ADDED: lsn# %" PRIu64, lsn));
                LocRecCtx->SyncLogRecovery->PutLogoBlob(LocRecCtx->VCtx->Top->GType, lsn, TLogoBlobID(id, 0), ingress);
            }

            return EDispatchStatus::Success;
        }

        EDispatchStatus HandleHugeBlobAllocChunk(const TActorContext &ctx, const NPDisk::TLogRecord &record) {
            bool success = HugeBlobAllocChunkRecoveryLogRec.ParseFromArray(record.Data.GetData(), record.Data.GetSize());
            if (!success)
                return EDispatchStatus::Error;

            ui64 lsn = record.Lsn;
            TRlas res = LocRecCtx->RepairedHuge->Apply(ctx, lsn, HugeBlobAllocChunkRecoveryLogRec);
            if (!res.Ok)
                return EDispatchStatus::Error;

            if (res.Skip) {
                LocRecCtx->RecovInfo->SkipHugeBlobAllocChunk();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (HUGE BLOB ALLOC CHUNK) SKIPPED: lsn# %" PRIu64, lsn));
            } else {
                LocRecCtx->RecovInfo->ApplyHugeBlobAllocChunk();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (HUGE BLOB ALLOC CHUNK) ADDED: lsn# %" PRIu64, lsn));

            }
            return EDispatchStatus::Success;
        }

        EDispatchStatus HandleHugeBlobFreeChunk(const TActorContext &ctx, const NPDisk::TLogRecord &record) {
            bool success = HugeBlobFreeChunkRecoveryLogRec.ParseFromArray(record.Data.GetData(), record.Data.GetSize());
            if (!success)
                return EDispatchStatus::Error;

            ui64 lsn = record.Lsn;
            TRlas res = LocRecCtx->RepairedHuge->Apply(ctx, lsn, HugeBlobFreeChunkRecoveryLogRec);
            if (!res.Ok)
                return EDispatchStatus::Error;

            if (res.Skip) {
                LocRecCtx->RecovInfo->SkipHugeBlobFreeChunk();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (HUGE BLOB FREE CHUNK) SKIPPED: lsn# %" PRIu64, lsn));
            } else {
                LocRecCtx->RecovInfo->ApplyHugeBlobFreeChunk();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (HUGE BLOB FREE CHUNK) ADDED: lsn# %" PRIu64, lsn));
            }
            return EDispatchStatus::Success;
        }


        EDispatchStatus HandleHugeBlobEntryPoint(const TActorContext &ctx, const NPDisk::TLogRecord &record) {
            ui64 lsn = record.Lsn;
            TRlas res = LocRecCtx->RepairedHuge->ApplyEntryPoint(ctx, lsn, record.Data.GetContiguousSpan());
            if (!res.Ok)
                return EDispatchStatus::Error;
            else
                return EDispatchStatus::Success;
        }

        EDispatchStatus HandleHugeLogoBlob(const TActorContext &ctx, const NPDisk::TLogRecord &record) {
            bool success = HugeBlobPutRecoveryLogRec.ParseFromArray(record.Data.GetData(), record.Data.GetSize());
            if (!success)
                return EDispatchStatus::Error;

            ui64 lsn = record.Lsn;
            TRlas res = LocRecCtx->RepairedHuge->Apply(ctx, lsn, HugeBlobPutRecoveryLogRec);
            if (!res.Ok)
                return EDispatchStatus::Error;

            if (res.Skip) {
                LocRecCtx->RecovInfo->SkipHugeLogoBlobToHeap();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (HUGE BLOB LOG) SKIPPED: lsn# %" PRIu64, lsn));
            } else {
                LocRecCtx->RecovInfo->ApplyHugeLogoBlobToHeap();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (HUGE BLOB LOG) ADDED: lsn# %" PRIu64, lsn));
            }

            PutHugeLogoBlobToHullAndSyncLog(ctx, record.Lsn,
                                            HugeBlobPutRecoveryLogRec.LogoBlobID,
                                            HugeBlobPutRecoveryLogRec.Ingress,
                                            HugeBlobPutRecoveryLogRec.DiskAddr);
            return EDispatchStatus::Success;
        }

        EDispatchStatus HandleHugeSlotsDelGeneric(const TActorContext &ctx, const NPDisk::TLogRecord &record,
                                       THullHugeKeeperPersState::ESlotDelDbType dbType) {
            TString explanation;
            NKikimrVDiskData::THullDbEntryPoint pb;
            const bool good = THullDbSignatureRoutines::ParseArray(pb, record.Data.GetData(), record.Data.GetSize(), explanation);
            if (!good)
                return EDispatchStatus::Error;

            HugeBlobs = TDiskPartVec(pb.GetRemovedHugeBlobs());
            ui64 lsn = record.Lsn;
            TRlas res = LocRecCtx->RepairedHuge->ApplySlotsDeletion(ctx, lsn, HugeBlobs, dbType);
            if (!res.Ok)
                return EDispatchStatus::Error;

            if (res.Skip) {
                LocRecCtx->RecovInfo->SkipHugeSlotsDelGeneric();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (HUGE BLOB FREE SLOT for %s) SKIPPED: lsn# %" PRIu64,
                                THullHugeKeeperPersState::SlotDelDbTypeToStr(dbType), lsn));
            } else {
                LocRecCtx->RecovInfo->ApplyHugeSlotsDelGeneric();
                LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                          VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                                "RECORD (HUGE BLOB FREE SLOT for %s) ADDED: lsn# %" PRIu64,
                                THullHugeKeeperPersState::SlotDelDbTypeToStr(dbType), lsn));
            }
            return EDispatchStatus::Success;
        }

        EDispatchStatus HandleHugeSlotsDelLogoBlobsDB(const TActorContext &ctx, const NPDisk::TLogRecord &record) {
            return HandleHugeSlotsDelGeneric(ctx, record, THullHugeKeeperPersState::LogoBlobsDb);
        }

        EDispatchStatus HandleHugeSlotsDelBlocksDB(const TActorContext &ctx, const NPDisk::TLogRecord &record) {
            return HandleHugeSlotsDelGeneric(ctx, record, THullHugeKeeperPersState::BlocksDb);
        }

        EDispatchStatus HandleHugeSlotsDelBarriersDB(const TActorContext &ctx, const NPDisk::TLogRecord &record) {
            return HandleHugeSlotsDelGeneric(ctx, record, THullHugeKeeperPersState::BarriersDb);
        }

        EDispatchStatus HandlePhantomLogoBlobs(const TActorContext& ctx, const NPDisk::TLogRecord& record) {
            const TRcBuf& data = record.Data;
            if (!PhantomLogoBlobs.ParseFromArray(data.GetData(), data.GetSize())) {
                return EDispatchStatus::Error;
            }

            ui64 lsn = record.Lsn - PhantomLogoBlobs.GetLogoBlobs().size() + 1;

            TIngress ingress;
            ingress.SetKeep(TIngress::IngressMode(LocRecCtx->VCtx->Top->GType), CollectModeDoNotKeep);
            for (const auto& id : PhantomLogoBlobs.GetLogoBlobs()) {
                LocRecCtx->RecovInfo->PhantomTryPutLogoBlob();
                const bool fromVPutCommand = false;
                PutLogoBlobToHullAndSyncLog(ctx, lsn, LogoBlobIDFromLogoBlobID(id), ingress, {}, fromVPutCommand);
                ++lsn;
            }
            return EDispatchStatus::Success;
        }

        EDispatchStatus HandleAnubisOsirisPut(const TActorContext& ctx, const NPDisk::TLogRecord& record) {
            if (!AnubisOsirisPutMsg.ParseFromArray(record.Data.GetData(), record.Data.GetSize())) {
                return EDispatchStatus::Error;
            }

            TEvAnubisOsirisPut put(AnubisOsirisPutMsg);
            THullDbInsert insert = put.PrepareInsert(LocRecCtx->VCtx->Top.get(), LocRecCtx->VCtx->ShortSelfVDisk);
            const bool fromVPutCommand = false;
            PutLogoBlobToHullAndSyncLog(ctx, record.Lsn, insert.Id, insert.Ingress, TString(), fromVPutCommand);
            return EDispatchStatus::Success;
        }

        EDispatchStatus HandleAddBulkSst(const TActorContext& ctx, const NPDisk::TLogRecord& record) {
            NKikimrVDiskData::TAddBulkSstRecoveryLogRec proto;
            if (!proto.ParseFromArray(record.Data.GetData(), record.Data.GetSize())) {
                return EDispatchStatus::Error;
            }

            const ui64 lsn = record.Lsn;
            const bool loadLogoBlobs = !LocRecCtx->HullDbRecovery->GetHullDs()->LogoBlobs->SkipRecord(lsn);
            const bool loadBlocks = !LocRecCtx->HullDbRecovery->GetHullDs()->Blocks->SkipRecord(lsn);
            const bool loadBarriers = !LocRecCtx->HullDbRecovery->GetHullDs()->Barriers->SkipRecord(lsn);
            const bool loadAtLeastOne = loadLogoBlobs || loadBlocks || loadBarriers;
            if (loadAtLeastOne) {
                IActor *actor = CreateBulkSstLoaderActor(LocRecCtx->VCtx, LocRecCtx->PDiskCtx, proto, ctx.SelfID,
                        lsn, loadLogoBlobs, loadBlocks, loadBarriers);
                auto aid = ctx.Register(actor);
                ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
                return EDispatchStatus::Async;
            } else {
                // skip record for all databases
                return EDispatchStatus::Success;
            }
        }

        EDispatchStatus HandleScrub(const TActorContext& /*ctx*/, const NPDisk::TLogRecord& /*record*/) {
            return EDispatchStatus::Success;
        }

        void Handle(TEvBulkSstEssenceLoaded::TPtr &ev, const TActorContext &ctx) {
            // BulkSstEssence is loaded into memory, apply it
            TEvBulkSstEssenceLoaded *msg = ev->Get();
            const ui64 lsn = msg->RecoveryLogRecLsn;
            TLsnSeg seg(lsn, lsn);
            LocRecCtx->HullDbRecovery->ReplayAddBulkSst(ctx, msg->Essence, seg, THullDbRecovery::RECOVERY);

            // continue applying log records
            InterruptableLogRecordsDispatch(ctx);
        }

        EDispatchStatus DispatchLogRecord(const TActorContext &ctx, const NPDisk::TLogRecord &record) {
            LOG_DEBUG(ctx, BS_LOCALRECOVERY,
                      VDISKP(LocRecCtx->VCtx->VDiskLogPrefix,
                            "DISPATCH RECORD: %s", record.ToString().data()));

            // Remember last seen lsn
            Y_ABORT_UNLESS(RecoveredLsn < record.Lsn,
                     "%s RecoveredLsn# %" PRIu64 " recordLsn# %" PRIu64 " signature# %" PRIu64,
                     LocRecCtx->VCtx->VDiskLogPrefix.data(), RecoveredLsn, record.Lsn, ui64(record.Signature));
            RecoveredLsn = record.Lsn;

            switch (record.Signature) {
                case TLogSignature::SignatureLogoBlob:
                    LocRecCtx->RecovInfo->DispatchSignatureLogoBlob(record);
                    return HandleLogoBlob(ctx, record);
                case TLogSignature::SignatureBlock:
                    LocRecCtx->RecovInfo->DispatchSignatureBlock(record);
                    return HandleBlock(ctx, record);
                case TLogSignature::SignatureGC:
                    LocRecCtx->RecovInfo->DispatchSignatureGC(record);
                    return HandleGC(ctx, record);
                case TLogSignature::SignatureSyncLogIdx:
                    LocRecCtx->RecovInfo->DispatchSignatureSyncLogIdx(record);
                    return EDispatchStatus::Success; // entry point, already handled
                case TLogSignature::SignatureHullLogoBlobsDB:
                    LocRecCtx->RecovInfo->DispatchSignatureHullLogoBlobsDB(record);
                    return HandleHugeSlotsDelLogoBlobsDB(ctx, record);
                case TLogSignature::SignatureHullBlocksDB:
                    LocRecCtx->RecovInfo->DispatchSignatureHullBlocksDB(record);
                    // entry point already handled, take care of huge slots
                    return HandleHugeSlotsDelBlocksDB(ctx, record);
                case TLogSignature::SignatureHullBarriersDB:
                    LocRecCtx->RecovInfo->DispatchSignatureHullBarriersDB(record);
                    // entry point already handled, take care of huge slots
                    return HandleHugeSlotsDelBarriersDB(ctx, record);
                case TLogSignature::SignatureHullCutLog:
                    LocRecCtx->RecovInfo->DispatchSignatureHullCutLog(record);
                    return EDispatchStatus::Success;
                case TLogSignature::SignatureLocalSyncData:
                    LocRecCtx->RecovInfo->DispatchSignatureLocalSyncData(record);
                    return HandleSyncData(ctx, record);
                case TLogSignature::SignatureSyncerState:
                    LocRecCtx->RecovInfo->DispatchSignatureSyncerState(record);
                    return EDispatchStatus::Success; // entry point, already handled
                case TLogSignature::SignatureHandoffDelLogoBlob:
                    LocRecCtx->RecovInfo->DispatchSignatureHandoffDelLogoBlob(record);
                    return HandleHandoffDel(ctx, record);
                case TLogSignature::SignatureHugeBlobAllocChunk:
                    LocRecCtx->RecovInfo->DispatchSignatureHugeBlobAllocChunk(record);
                    return HandleHugeBlobAllocChunk(ctx, record);
                case TLogSignature::SignatureHugeBlobFreeChunk:
                    LocRecCtx->RecovInfo->DispatchSignatureHugeBlobFreeChunk(record);
                    return HandleHugeBlobFreeChunk(ctx, record);
                case TLogSignature::SignatureHugeBlobEntryPoint:
                    LocRecCtx->RecovInfo->DispatchSignatureHugeBlobEntryPoint(record);
                    return HandleHugeBlobEntryPoint(ctx, record);
                case TLogSignature::SignatureHugeLogoBlob:
                    LocRecCtx->RecovInfo->DispatchSignatureHugeLogoBlob(record);
                    return HandleHugeLogoBlob(ctx, record);
                case TLogSignature::SignatureLogoBlobOpt:
                    LocRecCtx->RecovInfo->DispatchSignatureLogoBlobOpt(record);
                    return HandleOptLogoBlob(ctx, record);
                case TLogSignature::SignaturePhantomBlobs:
                    LocRecCtx->RecovInfo->DispatchSignaturePhantomBlobs(record);
                    return HandlePhantomLogoBlobs(ctx, record);
                case TLogSignature::SignatureAnubisOsirisPut:
                    LocRecCtx->RecovInfo->DispatchSignatureAnubisOsirisPut(record);
                    return HandleAnubisOsirisPut(ctx, record);
                case TLogSignature::SignatureAddBulkSst:
                    LocRecCtx->RecovInfo->DispatchSignatureAddBulkSst(record);
                    return HandleAddBulkSst(ctx, record);
                case TLogSignature::SignatureScrub:
                    LocRecCtx->RecovInfo->DispatchSignatureScrub(record);
                    return HandleScrub(ctx, record);
                case TLogSignature::Max:
                    break;
            }
            Y_FAIL_S("Unexpected case: " << record.Signature.ToString());
        }

        void VerifyOwnedChunks(const TActorContext& ctx) {
            TSet<TChunkIdx> chunks;

            // create a set of used chunks as seen from our side
            LocRecCtx->HullDbRecovery->GetOwnedChunks(chunks);
            LocRecCtx->RepairedHuge->GetOwnedChunks(chunks);
            LocRecCtx->SyncLogRecovery->GetOwnedChunks(chunks);

            // calculate leaked and unowned chunks
            TVector<TChunkIdx> leaks, misowned;
            std::set_difference(LocRecCtx->ReportedOwnedChunks.begin(), LocRecCtx->ReportedOwnedChunks.end(),
                    chunks.begin(), chunks.end(), std::back_inserter(leaks));
            std::set_difference(chunks.begin(), chunks.end(),
                    LocRecCtx->ReportedOwnedChunks.begin(), LocRecCtx->ReportedOwnedChunks.end(),
                    std::back_inserter(misowned));

            if (leaks || misowned) {
                TStringStream msg;
                msg << "VDISK FOUND INCONSISTENCY IN OWNED CHUNKS; ";
                    msg << "Leaked Chunks# [";
                for (size_t i = 0; i < leaks.size(); ++i) {
                    msg << (i ? " " : "") << leaks[i];
                }
                msg << "] Misowned Chunks# [";
                for (size_t i = 0; i < misowned.size(); ++i) {
                    msg << (i ? " " : "") << misowned[i];
                }
                msg << "]";

                LOG_CRIT(ctx, NKikimrServices::BS_LOCALRECOVERY,
                    VDISKP(LocRecCtx->VCtx->VDiskLogPrefix, "%s", msg.Str().data()));
            }
        }

        STRICT_STFUNC(StateFunc,
            HFunc(NPDisk::TEvReadLogResult, Handle)
            HFunc(TEvBulkSstEssenceLoaded, Handle)
            CFunc(NActors::TEvents::TSystem::PoisonPill, HandlePoison)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_DB_LOCAL_RECOVERY;
        }

        TRecoveryLogReplayer(TActorId parentId, std::shared_ptr<TLocalRecoveryContext> locRecCtx)
            : TActorBootstrapped<TRecoveryLogReplayer>()
            , ParentId(parentId)
            , LocRecCtx(std::move(locRecCtx))
            , HugeBlobAllocChunkRecoveryLogRec(0)
            , HugeBlobFreeChunkRecoveryLogRec(TVector<ui32>())
            , HugeBlobPutRecoveryLogRec(TLogoBlobID(), TIngress(), TDiskPart())
        {
            // store last indexed lsn (i.e. lsn of the last record that already in DiskRecLog)
            SyncLogMaxLsnStored = LocRecCtx->SyncLogRecovery->GetLastLsnOfIndexRecord();
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // CreateRecoveryLogReplayer
    ////////////////////////////////////////////////////////////////////////////
    IActor* CreateRecoveryLogReplayer(TActorId parentId, std::shared_ptr<TLocalRecoveryContext> locRecCtx) {
        return new TRecoveryLogReplayer(parentId, std::move(locRecCtx));
    }

} // NKikimr

