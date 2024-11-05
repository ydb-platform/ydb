#include "blobstorage_synclogkeeper_committer.h"
#include "blobstorage_synclogwriteparts.h"
#include "blobstorage_synclog_public_events.h"

#include <ydb/core/base/blobstorage_grouptype.h>

using namespace NKikimrServices;

namespace NKikimr {

    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogCommitterActor
        // The actor performes writes and commit
        ////////////////////////////////////////////////////////////////////////////
        class TSyncLogCommitterActor : public TActorBootstrapped<TSyncLogCommitterActor> {
            TIntrusivePtr<TSyncLogCtx> SlCtx;
            TSyncLogSnapshotPtr SyncLogSnap;
            const TActorId NotifyID;
            NPDisk::TCommitRecord CommitRecord;
            TEntryPointSerializer EntryPointSerializer;
            TMemRecLogSnapshotPtr SwapSnap;
            TIntrusivePtr<TWriteParts> Parts;
            ui32 SwapSnapPos = 0;

            // all appends to chunk we have made
            TDeltaToDiskRecLog Delta;
            const ui32 PageSize;
            const ui32 PagesInChunk;

            friend class TActorBootstrapped<TSyncLogCommitterActor>;
            static void* SyncLogCookie;

            void GenerateCommit(const TActorContext &ctx) {
                // serialize
                EntryPointSerializer.Serialize(Delta);

                // lsn
                TLsnSeg seg = SlCtx->LsnMngr->AllocLsnForLocalUse();
                // commit msg
                auto commitMsg = std::make_unique<NPDisk::TEvLog>(SlCtx->PDiskCtx->Dsk->Owner,
                        SlCtx->PDiskCtx->Dsk->OwnerRound, TLogSignature::SignatureSyncLogIdx,
                        CommitRecord, TRcBuf(EntryPointSerializer.GetSerializedData()), seg, nullptr);

                if (CommitRecord.CommitChunks || CommitRecord.DeleteChunks) {
                    LOG_INFO(ctx, NKikimrServices::BS_SKELETON,
                               VDISKP(SlCtx->VCtx->VDiskLogPrefix,
                                    "synclog commit CommitChunks# %s "
                                     " DeleteChunks# %s", FormatList(CommitRecord.CommitChunks).data(),
                                     FormatList(CommitRecord.DeleteChunks).data()));
                }

                LOG_DEBUG(ctx, BS_SYNCLOG,
                          VDISKP(SlCtx->VCtx->VDiskLogPrefix,
                                "COMMITTER: commit message: %s",
                                commitMsg->ToString().data()));
                LOG_DEBUG(ctx, NKikimrServices::BS_VDISK_CHUNKS,
                          VDISKP(SlCtx->VCtx->VDiskLogPrefix,
                                "COMMIT: PDiskId# %s Lsn# %" PRIu64 " type# SyncLog msg# %s",
                                SlCtx->PDiskCtx->PDiskIdString.data(), seg.Point(),
                                commitMsg->CommitRecord.ToString().data()));

                ctx.Send(SlCtx->LoggerID, commitMsg.release());
                Become(&TThis::StateCommit);
            }

            void FillInPortion(ui32 freePagesInChunk) {
                Parts->Clear();
                Y_DEBUG_ABORT_UNLESS(SwapSnap->Size() > SwapSnapPos);
                ui32 pagesLeft = ui32(SwapSnap->Size()) - SwapSnapPos;
                ui32 m = Min(freePagesInChunk, pagesLeft);
                for (ui32 i = 0; i < m; i++) {
                    TSyncLogPageSnap pageSnap = (*SwapSnap)[SwapSnapPos + i];
                    Parts->Push(pageSnap);
                }
            }

            void Bootstrap(const TActorContext &ctx) {
                if (!SwapSnap || SwapSnap->Empty()) {
                    GenerateCommit(ctx);
                } else {
                    // append to
                    ui32 lastChunkFreePages = SyncLogSnap->DiskSnapPtr->LastChunkFreePagesNum();
                    ui32 chunkIdx = 0;
                    ui32 offset = 0;
                    if (lastChunkFreePages > 0) {
                        // append to the chunk
                        Y_DEBUG_ABORT_UNLESS(SwapSnapPos == 0);
                        chunkIdx = SyncLogSnap->DiskSnapPtr->LastChunkIdx();
                        offset = (PagesInChunk - lastChunkFreePages) * PageSize;
                        FillInPortion(lastChunkFreePages);
                    } else {
                        // fill in the new chunk
                        chunkIdx = 0;
                        offset = 0;
                        FillInPortion(PagesInChunk);
                    }

                    // generate write
                    Parts->GenRefs();
                    Y_DEBUG_ABORT_UNLESS(Parts->Size());
                    NPDisk::TEvChunkWrite::TPartsPtr p(Parts.Get());
                    ctx.Send(SlCtx->PDiskCtx->PDiskId,
                             new NPDisk::TEvChunkWrite(SlCtx->PDiskCtx->Dsk->Owner, SlCtx->PDiskCtx->Dsk->OwnerRound,
                                                       chunkIdx, offset, p, SyncLogCookie,
                                                       true, NPriWrite::SyncLog));
                    LOG_DEBUG(ctx, BS_SYNCLOG,
                              VDISKP(SlCtx->VCtx->VDiskLogPrefix,
                                    "COMMITTER: initial write: chunkIdx# %" PRIu32, chunkIdx));
                    Become(&TThis::StateWrite);
                }
            }

            void Handle(NPDisk::TEvChunkWriteResult::TPtr &ev, const TActorContext &ctx) {
                LOG_DEBUG(ctx, BS_SYNCLOG,
                        VDISKP(SlCtx->VCtx->VDiskLogPrefix, "COMMITTER: write done"));
                CHECK_PDISK_RESPONSE(SlCtx->VCtx, ev, ctx);

                // chunk is written, apply index update
                ui32 chunkIdx = ev->Get()->ChunkIdx;
                Delta.AllAppends.emplace_back(chunkIdx, Parts->GetSnapPages());
                CommitRecord.CommitChunks.push_back(chunkIdx);

                // continue writing or commit
                SwapSnapPos += Parts->GetSnapPages().size();
                if (SwapSnapPos == SwapSnap->Size()) {
                    GenerateCommit(ctx);
                } else {
                    ui32 chunkIdx = 0;
                    ui32 offset = 0;
                    FillInPortion(PagesInChunk);
                    // generate write
                    Parts->GenRefs();
                    Y_DEBUG_ABORT_UNLESS(Parts->Size());
                    NPDisk::TEvChunkWrite::TPartsPtr p(Parts.Get());
                    ctx.Send(SlCtx->PDiskCtx->PDiskId,
                             new NPDisk::TEvChunkWrite(SlCtx->PDiskCtx->Dsk->Owner, SlCtx->PDiskCtx->Dsk->OwnerRound,
                                                       chunkIdx, offset, p, SyncLogCookie,
                                                       true, NPriWrite::SyncLog));
                    LOG_DEBUG(ctx, BS_SYNCLOG,
                              VDISKP(SlCtx->VCtx->VDiskLogPrefix,
                                    "COMMITTER: next write: chunkIdx# %" PRIu32, chunkIdx));
                }
            }

            void Handle(NPDisk::TEvLogResult::TPtr &ev, const TActorContext &ctx) {
                CHECK_PDISK_RESPONSE(SlCtx->VCtx, ev, ctx);
                Y_ABORT_UNLESS(ev->Get()->Results.size() == 1);
                const ui64 entryPointLsn = ev->Get()->Results[0].Lsn;
                TCommitHistory commitHistory(TAppData::TimeProvider->Now(), entryPointLsn, EntryPointSerializer.RecoveryLogConfirmedLsn);
                ctx.Send(NotifyID, new TEvSyncLogCommitDone(commitHistory,
                    EntryPointSerializer.GetEntryPointDbgInfo(), std::move(Delta)));
                Die(ctx);
            }

            void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
                Y_UNUSED(ev);
                Die(ctx);
            }

            STRICT_STFUNC(StateWrite,
                          HFunc(NPDisk::TEvChunkWriteResult, Handle)
                          HFunc(TEvents::TEvPoisonPill, HandlePoison)
                          )

            STRICT_STFUNC(StateCommit,
                          HFunc(NPDisk::TEvLogResult, Handle)
                          HFunc(TEvents::TEvPoisonPill, HandlePoison)
                          )

            PDISK_TERMINATE_STATE_FUNC_DEF;

        public:
            static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
                return NKikimrServices::TActivity::BS_SYNCLOG_COMMITTER;
            }

            TSyncLogCommitterActor(
                    TIntrusivePtr<TSyncLogCtx> slCtx,
                    const TActorId &notifyID,
                    TSyncLogKeeperCommitData &&commitData)
                : TActorBootstrapped<TSyncLogCommitterActor>()
                , SlCtx(std::move(slCtx))
                , SyncLogSnap(std::move(commitData.SyncLogSnap))
                , NotifyID(notifyID)
                , CommitRecord()
                , EntryPointSerializer(
                    SyncLogSnap,
                    std::move(commitData.ChunksToDeleteDelayed),
                    commitData.RecoveryLogConfirmedLsn)
                , SwapSnap(std::move(commitData.SwapSnap))
                , Parts(new TWriteParts(SyncLogSnap->DiskSnapPtr->AppendBlockSize))
                , Delta(SyncLogSnap->DiskSnapPtr->IndexBulk)
                , PageSize(SyncLogSnap->DiskSnapPtr->AppendBlockSize)
                , PagesInChunk(SyncLogSnap->DiskSnapPtr->PagesInChunk)
            {
                CommitRecord.DeleteChunks = std::move(commitData.ChunksToDelete);
                CommitRecord.IsStartingPoint = true;
                Parts->Reserve(PagesInChunk);
            }
        };

        void *TSyncLogCommitterActor::SyncLogCookie = (void *)"SyncLog";


        IActor *CreateSyncLogCommitter(
                TIntrusivePtr<TSyncLogCtx> slCtx,
                const TActorId &notifyID,
                TSyncLogKeeperCommitData &&commitData)
        {
            return new TSyncLogCommitterActor(std::move(slCtx), notifyID, std::move(commitData));
        }

    } // NSyncLog

} // NKikimr
