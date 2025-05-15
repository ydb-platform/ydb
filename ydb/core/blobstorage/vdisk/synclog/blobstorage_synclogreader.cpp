#include "blobstorage_synclogreader.h"
#include "blobstorage_synclogdata.h"
#include "blobstorage_synclogmsgwriter.h"
#include "blobstorage_synclogkeeper.h"
#include "blobstorage_synclogrecovery.h"
#include "blobstorage_syncloghttp.h"
#include "blobstorage_synclog_private_events.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>
#include <ydb/core/blobstorage/base/vdisk_priorities.h>

using namespace NKikimrServices;
using namespace NKikimr::NSyncLog;

namespace NKikimr {
    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // SYNC LOGIC
        ////////////////////////////////////////////////////////////////////////////
        const char *Name2Str(EReadWhatsNext w) {
            switch (w) {
                case EWnError:      return "Error";
                case EWnFullSync:   return "FullSync";
                case EWnDiskSynced: return "DiskSynced";
                case EWnMemRead:    return "MemRead";
                case EWnDiskRead:   return "DiskRead";
                default:            return "Unknown";
            }
        }

        TWhatsNextOutcome WhatsNext(ui64 syncedLsn,
                                    ui64 dbBirthLsn,
                                    const NSyncLog::TLogEssence *e,
                                    std::function<TString()> reportInternals) {
            // NOTE: we use dbBirthLsn to handle read requests, below I describe why.
            //       After lost data recovery, we would like syncer to start with DbBirthLsn,
            //       because there is no need to sync previous records (they are obtained during
            //       full sync and other VDisks already have them). Unfortunately internal
            //       SyncLog structures start from 0 (because there were no puts). We can update
            //       these internal structure by committing a new entry point for SyncLog,
            //       but we decided to use DbBirthLsn that we got from Syncer only for calculating
            //       reply to read message. It seems simpler, no changes to db local recovery
            //       process, no new records, just calculations.
            ui64 logStartLsn = Max(e->LogStartLsn, dbBirthLsn + 1);

            // check that syncedLsn is enough to sync from log
            if (syncedLsn + 1 < logStartLsn) {
                return TWhatsNextOutcome(EWnFullSync);
            } else {
                if (e->MemLogEmpty && e->DiskLogEmpty) {
                    // SyncLog is empty, we can only compare LogStartLsn with syncedLsn
                    if ((syncedLsn + 1 == logStartLsn) || (syncedLsn == 0 && logStartLsn == 0)) {
                        return TWhatsNextOutcome(EWnDiskSynced);
                    } else {
                        return TWhatsNextOutcome::Error(1);
                    }
                } else {
                    // Log is not empty
                    ui64 lastLogLsn = (!e->MemLogEmpty ? e->LastMemLsn : e->LastDiskLsn);
                    ui64 firstLogLsn = 0;

                    if (!e->MemLogEmpty && !e->DiskLogEmpty) {
                        Y_ABORT_UNLESS(e->FirstDiskLsn <= e->FirstMemLsn, "%s", reportInternals().data());
                        firstLogLsn = e->FirstDiskLsn;
                    } else if (e->MemLogEmpty) {
                        firstLogLsn = e->FirstDiskLsn;
                    } else if (e->DiskLogEmpty) {
                        firstLogLsn = e->FirstMemLsn;
                    }

                    Y_ABORT_UNLESS(lastLogLsn != 0 && firstLogLsn <= lastLogLsn,
                             " firstLogLsn# %" PRIu64 " lastLogLsn# %" PRIu64 " %s",
                             firstLogLsn, lastLogLsn, reportInternals().data());

                    if (!(syncedLsn + 1 >= logStartLsn)) {
                        return TWhatsNextOutcome::Error(2);
                    }

                    if (lastLogLsn + 1 <= logStartLsn) {
                        if (syncedLsn + 1 == logStartLsn) {
                            return TWhatsNextOutcome(EWnDiskSynced);
                        } else {
                            return TWhatsNextOutcome::Error(3);
                        }
                    } else {
                        // lastLogLsn >= logStartLsn
                        if (syncedLsn == lastLogLsn) {
                            return TWhatsNextOutcome(EWnDiskSynced);
                        } else if (syncedLsn > lastLogLsn) {
                            return TWhatsNextOutcome::Error(4);
                        } else {
                            if (e->DiskLogEmpty) {
                                return TWhatsNextOutcome(EWnMemRead);
                            } else {
                                // DiskIndex is not empty
                                if (e->MemLogEmpty) {
                                    return TWhatsNextOutcome(EWnDiskRead);
                                } else {
                                    // both indexes are not empty
                                    if (syncedLsn >= Min(e->FirstMemLsn, e->LastDiskLsn)
                                        || e->FirstMemLsn == e->FirstDiskLsn) {
                                        return TWhatsNextOutcome(EWnMemRead);
                                    } else {
                                        return TWhatsNextOutcome(EWnDiskRead);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }


        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogFilter
        ////////////////////////////////////////////////////////////////////////////
        struct TSyncLogFilter : private TLogoBlobFilter {
            TSyncLogFilter(const std::shared_ptr<TBlobStorageGroupInfo::TTopology> &top,
                           const TVDiskID &vdisk)
                : TLogoBlobFilter(top, vdisk)
            {}

            bool Check(const TRecordHdr *rec) const {
                return (rec->RecType != TRecordHdr::RecLogoBlob)
                    || TLogoBlobFilter::Check(TLogoBlobID(rec->GetLogoBlob()->Raw));
            }
        };


        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogReaderActor
        ////////////////////////////////////////////////////////////////////////////
        class TSyncLogReaderActor : public TActorBootstrapped<TSyncLogReaderActor> {
            TIntrusivePtr<TSyncLogCtx> SlCtx;
            const TVDiskIncarnationGuid VDiskIncarnationGuid;
            TEvBlobStorage::TEvVSync::TPtr Ev;
            const TActorId ParentId;
            const TActorId KeeperId;
            const TVDiskID SelfVDiskId;
            const TVDiskID SourceVDisk;
            TSyncLogSnapshotPtr SnapPtr;
            TLz4FragmentWriter FragmentWriter;
            TSyncLogFilter Filter;
            TDiskRecLogSnapshot::TIndexRecIterator DiskIt;
            ui64 LastLsn;
            ui32 DiskReads;
            const ui64 DbBirthLsn;
            const TInstant Now;

            friend class TActorBootstrapped<TSyncLogReaderActor>;

            void Bootstrap(const TActorContext &ctx) {
                Become(&TThis::StateInitFunc);
                ctx.Send(KeeperId, new TEvSyncLogSnapshot());
            }

            void Finish(const TActorContext &ctx,
                        NKikimrProto::EReplyStatus status,
                        ui64 lsn,
                        bool finished) {
                LOG_DEBUG(ctx, BS_SYNCLOG,
                          VDISKP(SlCtx->VCtx->VDiskLogPrefix,
                                "SYNCLOG REPLY: SourceVDisk# %s guid# %" PRIu64 " lsn# %" PRIu64,
                                SourceVDisk.ToString().data(), static_cast<ui64>(VDiskIncarnationGuid), lsn));

                auto result = std::make_unique<TEvBlobStorage::TEvVSyncResult>(status, SelfVDiskId,
                    TSyncState(VDiskIncarnationGuid, lsn), finished, SlCtx->VCtx->GetOutOfSpaceState().GetLocalStatusFlags(),
                    Now, SlCtx->IFaceMonGroup.SyncReadResMsgsPtr(), nullptr, Ev->GetChannel());
                if (DiskReads) {
                    NKikimrBlobStorage::TEvVSyncResult::TStat *stat = nullptr;
                    stat = result->Record.MutableStat();
                    stat->SetDiskReads(DiskReads);
                }
                if (FragmentWriter.GetSize()) {
                    FragmentWriter.Finish(result->Record.MutableData());
                }

                SendVDiskResponse(ctx, Ev->Sender, result.release(), Ev->Cookie, SlCtx->VCtx, {});
                ctx.Send(ParentId, new TEvSyncLogReadFinished(SourceVDisk));
                Die(ctx);
            }

            void Handle(TEvSyncLogSnapshotResult::TPtr &ev, const TActorContext &ctx) {
                SnapPtr = ev->Get()->SnapshotPtr;
                ProcessReadRequest(ctx);
            }

            static TString InternalsToString(const TEvBlobStorage::TEvVSync *ev,
                                            const TSyncLogSnapshot *slSnap,
                                            ui64 dbBirthLsn)
            {
                const NKikimrBlobStorage::TEvVSync &record = ev->Record;
                TStringStream str;
                str << "syncState# " << SyncStateFromSyncState(record.GetSyncState()).ToString()
                    << " dbBirthLsn# " << dbBirthLsn
                    << " boundaries# " << slSnap->BoundariesToString()
                    << " sourceVDisk# " << VDiskIDFromVDiskID(record.GetSourceVDiskID())
                    << " targetVDisk# " << VDiskIDFromVDiskID(record.GetTargetVDiskID());
                return str.Str();
            }

            void ProcessErrorOutcome(const TActorContext &ctx, const TWhatsNextOutcome &wno) {
                ++SlCtx->CountersMonGroup.ReplyError();
                {
                    // verbose error notification
                    TStringStream str;
                    str << "SYNCLOG LOGIC ERROR: " << wno.Explanation
                        << " " << InternalsToString(Ev->Get(), SnapPtr.Get(), DbBirthLsn);
                    LOG_ERROR(ctx, BS_SYNCLOG,  str.Str());
                    // Y_ABORT("%s", str.Str().data()); // TODO(alexvru): fix logic
                }
                Finish(ctx, NKikimrProto::ERROR, 0, true);
            }

            void ProcessFullSyncOutcome(const TActorContext &ctx, const TWhatsNextOutcome &wno) {
                Y_UNUSED(wno);
                LOG_WARN(ctx, BS_SYNCLOG,
                         VDISKP(SlCtx->VCtx->VDiskLogPrefix,
                                "Handle(TEvSyncLogRead): FULL_RECOVER(no data); %s",
                                InternalsToString(Ev->Get(), SnapPtr.Get(), DbBirthLsn).data()));
                ++SlCtx->CountersMonGroup.FullRecovery();
                Finish(ctx, NKikimrProto::NODATA, DbBirthLsn, true);
            }

            void ProcessGoodOutcome(const TActorContext &ctx,
                                    const TWhatsNextOutcome &wno,
                                    ui64 syncedLsn) {
                ++SlCtx->CountersMonGroup.NormalSync();
                LOG_DEBUG(ctx, BS_SYNCLOG,
                          VDISKP(SlCtx->VCtx->VDiskLogPrefix,
                                "Handle(TEvSyncLogRead): OK; whatsNext# %s %s",
                                Name2Str(wno.WhatsNext),
                                InternalsToString(Ev->Get(), SnapPtr.Get(), DbBirthLsn).data()));

                switch (wno.WhatsNext) {
                    case EWnDiskSynced:
                        Finish(ctx, NKikimrProto::ALREADY, syncedLsn, true);
                        break;
                    case EWnMemRead:
                        // get data from log
                        LastLsn = syncedLsn;
                        ReadFromMemory(ctx);
                        break;
                    case EWnDiskRead:
                        // get data from log
                        LastLsn = syncedLsn;
                        ReadFromDisk(ctx);
                        break;
                    default:
                        Y_ABORT("Unexpected value# %d", int(wno.WhatsNext));
                }
            }

            void ProcessReadRequest(const TActorContext &ctx) {
                const NKikimrBlobStorage::TEvVSync &record = Ev->Get()->Record;
                TSyncState oldSyncState = SyncStateFromSyncState(record.GetSyncState());
                ui64 syncedLsn = oldSyncState.SyncedLsn;

                // logic (what to do next)
                TLogEssence e {};
                SnapPtr->FillInLogEssence(&e);
                auto ri = std::bind(InternalsToString, Ev->Get(), SnapPtr.Get(), DbBirthLsn);
                TWhatsNextOutcome wno = WhatsNext(syncedLsn, DbBirthLsn, &e, ri);

                // process outcome
                if (wno.WhatsNext == EWnError) {
                    ProcessErrorOutcome(ctx, wno);
                } else if (wno.WhatsNext == EWnFullSync) {
                    ProcessFullSyncOutcome(ctx, wno);
                } else {
                    ProcessGoodOutcome(ctx, wno, syncedLsn);
                }
            }

            // returns true if continue, false otherwise
            bool ProcessRecord(const TRecordHdr *hdr) {
                if (hdr->Lsn > LastLsn) {
                    if (Filter.Check(hdr)) {
                        size_t hdrSize = hdr->GetSize();
                        if ((FragmentWriter.GetSize() + hdrSize) < SlCtx->MaxResponseSize)
                            FragmentWriter.Push(hdr, hdrSize);
                        else
                            return false;
                    }
                    LastLsn = hdr->Lsn;
                }
                return true;
            }

            void ReadFromMemory(const TActorContext &ctx) {
                // start from already synced lsn + 1
                TMemRecLogSnapshot::TIterator it(SnapPtr->MemSnapPtr);
                it.Seek(LastLsn + 1);

                bool cont = true;
                while (it.Valid() && cont) {
                    const TRecordHdr *hdr = it.Get();
                    cont = ProcessRecord(hdr);
                    it.Next();
                }
                bool finished = cont && !it.Valid();
                Finish(ctx, NKikimrProto::OK, LastLsn, finished);
            }

            void ReadFromDisk(const TActorContext &ctx) {
                // make iterator
                DiskIt = TDiskRecLogSnapshot::TIndexRecIterator(SnapPtr->DiskSnapPtr);
                DiskIt.Seek(LastLsn + 1);
                // start reading
                Become(&TThis::StateReadFunc);
                NextReadChunkOp(ctx);
            }

            void NextReadChunkOp(const TActorContext &ctx) {
                if (!DiskIt.Valid()) {
                    ReadFromMemory(ctx);
                } else {
                    std::pair<ui32, const TDiskIndexRecord *> p = DiskIt.Get();
                    ui32 chunkIdx = p.first;
                    const TDiskIndexRecord *idxRec = p.second;

                    const ui32 offs = idxRec->OffsetInPages * SnapPtr->AppendBlockSize;
                    const ui32 size = idxRec->PagesNum * SnapPtr->AppendBlockSize;
                    // monitoring
                    ++SlCtx->CountersMonGroup.ReadsFromDisk();
                    SlCtx->CountersMonGroup.ReadsFromDiskBytes() += size;
                    DiskReads++;
                    // send read request
                    ctx.Send(SlCtx->PDiskCtx->PDiskId,
                             new NPDisk::TEvChunkRead(SlCtx->PDiskCtx->Dsk->Owner, SlCtx->PDiskCtx->Dsk->OwnerRound,
                                                      chunkIdx, offs, size,
                                                      NPriRead::SyncLog, nullptr));
                }
            }

            void Handle(NPDisk::TEvChunkReadResult::TPtr &ev, const TActorContext &ctx) {
                // FIXME: optimize, batch reads; use Db->RecommendedReadSize
                CHECK_PDISK_RESPONSE_READABLE(SlCtx->VCtx, ev, ctx);

                Y_ABORT_UNLESS(DiskIt.Valid());
                std::pair<ui32, const TDiskIndexRecord *> p = DiskIt.Get();
                ui32 chunkIdx = p.first;
                const TDiskIndexRecord *idxRec = p.second;
                auto msg = ev->Get();
                const TBufferWithGaps &readData = ev->Get()->Data;
                Y_ABORT_UNLESS(chunkIdx == msg->ChunkIdx &&
                         idxRec->OffsetInPages * SnapPtr->AppendBlockSize == msg->Offset &&
                         idxRec->PagesNum * SnapPtr->AppendBlockSize == readData.Size(),
                         "SyncLog read command failed: chunkIdx# %" PRIu32
                         " msgChunkIdx# %" PRIu32 " OffsetInPages# %" PRIu32
                         " appendBlockSize# %" PRIu32 " msgOffset# %" PRIu32
                         " PagesNum# %" PRIu32 " readDataSize# %" PRIu32,
                         chunkIdx, msg->ChunkIdx, idxRec->OffsetInPages, SnapPtr->AppendBlockSize,
                         msg->Offset, idxRec->PagesNum, ui32(readData.Size()));

                // process all pages
                for (ui32 pi = 0; pi < idxRec->PagesNum; pi++) {
                    const TSyncLogPage *page = nullptr;
                    page = readData.DataPtr<const TSyncLogPage>(pi * SnapPtr->AppendBlockSize,
                                                                SnapPtr->AppendBlockSize);

                    // process through one page
                    TSyncLogPageROIterator hi(page);
                    hi.SeekToFirst();
                    bool cont = true;
                    while (hi.Valid() && cont) {
                        const TRecordHdr *hdr = hi.Get();
                        cont = ProcessRecord(hdr);
                        hi.Next();
                    }

                    if (!cont) {
                        // message is filled, terminate
                        Finish(ctx, NKikimrProto::OK, LastLsn, false);
                        return;
                    }
                }

                // all pages have been processed
                DiskIt.Next();
                NextReadChunkOp(ctx);
            }

            void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
                Y_UNUSED(ev);
                Die(ctx);
            }

            STRICT_STFUNC(StateInitFunc,
                HFunc(TEvSyncLogSnapshotResult, Handle)
                HFunc(TEvents::TEvPoisonPill, HandlePoison)
            )

            STRICT_STFUNC(StateReadFunc,
                HFunc(NPDisk::TEvChunkReadResult, Handle)
                HFunc(TEvents::TEvPoisonPill, HandlePoison)
            )

            PDISK_TERMINATE_STATE_FUNC_DEF;

        public:
            static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
                return NKikimrServices::TActivity::BS_SYNCLOG_READER;
            }

            TSyncLogReaderActor(
                    const TIntrusivePtr<TSyncLogCtx> &slCtx,
                    const TVDiskIncarnationGuid vdiskIncarnationGuid,
                    TEvBlobStorage::TEvVSync::TPtr &ev,
                    const TActorId &parentId,
                    const TActorId &keeperId,
                    const TVDiskID &selfVDiskId,
                    const TVDiskID &sourceVDisk,
                    ui64 dbBirthLsn,
                    TInstant now)
                : TActorBootstrapped<TSyncLogReaderActor>()
                , SlCtx(slCtx)
                , VDiskIncarnationGuid(vdiskIncarnationGuid)
                , Ev(ev)
                , ParentId(parentId)
                , KeeperId(keeperId)
                , SelfVDiskId(selfVDiskId)
                , SourceVDisk(sourceVDisk)
                , SnapPtr()
                , FragmentWriter()
                , Filter(SlCtx->VCtx->Top, sourceVDisk)
                , DiskIt()
                , LastLsn(0)
                , DiskReads(0)
                , DbBirthLsn(dbBirthLsn)
                , Now(now)
            {}
        };

        ////////////////////////////////////////////////////////////////////////////
        // CreateSyncLogReaderActor
        ////////////////////////////////////////////////////////////////////////////
        IActor* CreateSyncLogReaderActor(
            const TIntrusivePtr<TSyncLogCtx> &slCtx,
            const TVDiskIncarnationGuid vdiskIncarnationGuid,
            TEvBlobStorage::TEvVSync::TPtr &ev,
            const TActorId &parentId,
            const TActorId &keeperId,
            const TVDiskID &selfVDiskId,
            const TVDiskID &sourceVDisk,
            ui64 dbBirthLsn,
            TInstant now)
        {
            return new TSyncLogReaderActor(slCtx, vdiskIncarnationGuid, ev, parentId, keeperId,
                selfVDiskId, sourceVDisk, dbBirthLsn, now);
        }


    } // NSyncLog
} // NKikimr

